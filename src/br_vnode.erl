-module(br_vnode).
-behaviour(riak_core_vnode).
-include("bloomd_ring.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-ifdef(TEST).
-compile(export_all).
-endif.

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

% Internal API, exported for use in other modules
% such as rpc
-export([local_command/3]).

-record(state, {
        % Partition number
        partition,

        % Index number
        idx,

        % Slice pattern
        re,

        % Connection to local bloomd
        conn,

        % Handoff state
        handoff=false
        }).

% This is the replication factor
-define(N, 3).


%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    % Determine our v-node number
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    Chash = riak_core_ring:chash(R),
    {NumPartitions, _} = Chash,
    Incr = chash:ring_increment(NumPartitions),
    Idx = trunc(Partition / Incr),

    % Create a slice match pattern that matches the index and colon prefix
    {ok, Re} = re:compile(iolist_to_binary(["^", integer_to_list(Idx), ":(.*)$"])),

    {ok, LocalHost} = application:get_env(bloomd_ring, bloomd_local_host),
    {ok, LocalPort} = application:get_env(bloomd_ring, bloomd_local_port),
    {ok, PoolSize} = application:get_env(bloomd_ring, bloomd_worker_pool_size),

    % Try to connect to the local bloomd without key hashing enabled
    Conn = bloomd:new(LocalHost, LocalPort, false),

    % Configure the worker pool
    Pool = {pool, br_worker, PoolSize, []},

    % Setup our state
    {ok, #state{partition=Partition, idx=Idx, re=Re, conn=Conn}, [Pool]}.


%%
% Handles the check key command. This can be sent down to bloomd
% directly, since we know exactly which slice handles it.
%%%
handle_command({check_filter, FilterName, Slice, Key}, Sender, State) ->
    % Make use of pipelining instead of blocking the v-node
    spawn(fun() ->
        % Convert into the proper names
        Name = filter_slice_name(State#state.idx, FilterName, Slice),

        % Query bloomd
        F = bloomd:filter(State#state.conn, Name),
        Res = bloomd:check(F, Key),

        % Get the response
        Resp = case Res of
            {ok, [V]} -> {ok, V};
            {error, E} -> {error, E}
        end,

        % Respond
        riak_core_vnode:reply(Sender, Resp)
    end),

    % Do not respond, other process will do it
    {noreply, State};


%%%
% Handles the set key command. This can be sent down to bloomd
% directly, since we know exactly which slice handles it.
%%%
handle_command({set_filter, FilterName, Slice, Key}, Sender, State) ->
    % Make use of pipelining instead of blocking the v-node
    spawn(fun() ->
        % Convert into the proper names
        Name = filter_slice_name(State#state.idx, FilterName, Slice),

        % Query bloomd
        F = bloomd:filter(State#state.conn, Name),
        Res = bloomd:set(F, Key),

        % Get the response
        Resp = case Res of
            {ok, [V]} -> {ok, V};
            {error, E} -> {error, E}
        end,

        % Respond
        riak_core_vnode:reply(Sender, Resp)
    end),

    % Do not respond, other process will do it
    {noreply, State};


%%%
% Handles the create filter command. This command takes the slices
% to create, since a particular v-node may be responsible for multiple
% slices. It also takes an options list that should be passed down.
%
% We should respond with one of done, exists, or {error, command_failed}
%%%
handle_command({create_filter, FilterName, Options}, _Sender, State) ->
    % Generate a list of {Slice, Hash} for each slice
    NumPartitions = br_util:num_partitions(),
    Indices = [{Slice, br_util:hash_slice(FilterName, Slice)}
               || Slice <- lists:seq(0, NumPartitions-1)],

    % Determine the preflist for each slice
    Preflists = [{Slice, riak_core_apl:get_primary_apl(Idx, ?N, bloomd)} || {Slice, Idx} <- Indices],

    % Get just the indexes
    PrefNodes = [{S, [Idx || {{Idx, _}, _} <- Pref]} || {S, Pref} <- Preflists],

    % Get the owned slices for this partition
    Owned = [Slice || {Slice, Partitions} <- PrefNodes, lists:member(State#state.partition, Partitions)],

    % Convert into the proper names
    Names = [filter_slice_name(State#state.idx, FilterName, S) || S <- Owned],

    % Execute all the creates in parallel
    Results = rpc:pmap({br_vnode, local_command}, [{create, Options}, State], Names),

    % Check for any errors
    Resp = case has_error(Results, [command_failed, internal_error]) of
        true -> {error, command_failed};
        _ ->
            % Check if all the slices exist
            AllExist = lists:all(fun(exists) -> true; (_) -> false end, Results),
            case AllExist of
                true -> exists;
                _ ->
                    AllClient = lists:all(fun({error, {client_error, _}}) -> true;
                                (_) -> false end, Results),
                    case AllClient of
                        true -> [Err|_] = Results, Err;
                        _ -> done
                    end
            end
    end,

    % Respond
    {reply, Resp, State};


%%%
% Handles the list filter command. This command queries the
% local bloomd for all the slices and provides per-slice
% information that the co-ordinators can then use to respond.
%%%
handle_command(list_filters, _Sender, State) ->
    % Find all the filters
    Results = bloomd:list(State#state.conn),

    % Check for error
    Resp = case Results of
        {error, _} -> {error, command_failed};
        _ ->
            % Extract the filter info
            % {{FilterName, Idx, Slice}, Info}
            FilterInfoList = [{filter_slice_value(F), bloomd:filter_info(I)} || {F, I} <- Results],

            % Collapse the duplicates by name
            FilterCombined = lists:foldl(fun({{Name, Idx, Slice}, Info}, Accum) ->
                case State#state.idx of
                    Idx -> dict:append(Name, {Slice, Info}, Accum);
                    _ -> Accum
                end
            end, dict:new(), FilterInfoList),

            % Return the slice info
            {ok, dict:to_list(FilterCombined)}
    end,

    % Repond
    {reply, Resp, State};


%%%
% Handles the drop filter command. This command queries
% the local bloomd for all the slices that match the given
% fitler name, and issues a drop for all of the slices.
%%%
handle_command({drop_filter, FilterName}, _Sender, State) ->
    Resp = case matching_slices(FilterName, State) of
        {error, _} -> {error, command_failed};
        [] -> {error, no_filter};
        Slices ->
            % Execute all the drops in parallel
            DropResults = rpc:pmap({br_vnode, local_command}, [drop, State], Slices),

            % Check for any errors
            case has_error(DropResults, [command_failed, internal_error]) of
                true -> {error, command_failed};
                _ -> done
            end
    end,

    % Repond
    {reply, Resp, State};


%%%
% Handles the close filter command. This command queries
% the local bloomd for all the slices that match the given
% fitler name, and issues a close for all of the slices.
%%%
handle_command({close_filter, FilterName}, _Sender, State) ->
    Resp = case matching_slices(FilterName, State) of
        {error, _} -> {error, command_failed};
        [] -> {error, no_filter};
        Slices ->
            % Execute all the closes in parallel
            CloseResults = rpc:pmap({br_vnode, local_command}, [close, State], Slices),

            % Check for any errors
            case has_error(CloseResults, [command_failed, internal_error]) of
                true -> {error, command_failed};
                _ -> done
            end
    end,

    % Repond
    {reply, Resp, State};


%%%
% Handles the clear filter command. This command queries
% the local bloomd for all the slices that match the given
% fitler name, and issues a clear for all of the slices.
%%%
handle_command({clear_filter, FilterName}, _Sender, State) ->
    Resp = case matching_slices(FilterName, State) of
        {error, _} -> {error, command_failed};
        [] -> {error, no_filter};
        Slices ->
            % Execute all the clears in parallel
            ClearResults = rpc:pmap({br_vnode, local_command}, [clear, State], Slices),

            % Check for any errors
            case has_error(ClearResults, [command_failed, internal_error]) of
                true -> {error, command_failed};
                _ ->
                    % Check if any of the filters were not proxied
                    case has_error(ClearResults, [not_proxied]) of
                        true ->
                            % Some of the slices were not proxied, so we should
                            % make a best effort to roll this back. We do this by
                            % issuing an async create on the slices.
                            spawn(fun() ->rpc:pmap({br_vnode, local_command}, [{create, []}, State], Slices) end),
                            {error, not_proxied};

                        _ -> done
                    end
            end
    end,

    % Repond
    {reply, Resp, State};


%%%
% Handles the flush command. This command queries
% the local bloomd for all the slices that match the given
% fitler name, and issues a flush for all of the slices.
%%%
handle_command({flush_filter, FilterName}, _Sender, State) ->
    Resp = case FilterName of
        undefined ->
            % Find all the filters
            Results = bloomd:list(State#state.conn),

            % Check for error
            case any_error(Results) of
                true -> {error, command_failed};
                _ ->
                    % Find all the matching slices
                    Slices = [F || {F, _I} <- Results],

                    % Execute all the closes in parallel
                    CloseResults = rpc:pmap({br_vnode, local_command}, [flush, State], Slices),

                    % Check for any errors
                    case has_error(CloseResults, [command_failed, internal_error]) of
                        true -> {error, command_failed};
                        _ -> done
                    end
            end;

        _ ->
            case matching_slices(FilterName, State) of
                {error, _} -> {error, command_failed};
                [] -> {error, no_filter};
                Slices ->
                    % Execute all the closes in parallel
                    CloseResults = rpc:pmap({br_vnode, local_command}, [flush, State], Slices),

                    % Check for any errors
                    case has_error(CloseResults, [command_failed, internal_error]) of
                        true -> {error, command_failed};
                        _ -> done
                    end
            end
    end,

    % Repond
    {reply, Resp, State};


%%%
% Handles the info command. This command queries
% the local bloomd for all the slices that match the given
% fitler name, and issues an info for all of the slices.
%%%
handle_command({info_filter, FilterName}, _Sender, State) ->
    Resp = case matching_slices(FilterName, State) of
        {error, _} -> {error, command_failed};
        [] -> {error, no_filter};
        Slices ->
            % Execute all the infos in parallel
            InfoResults = rpc:pmap({br_vnode, local_command}, [info, State], Slices),

            % Check for any errors
            case has_error(InfoResults, [command_failed, internal_error]) of
                true -> {error, command_failed};
                _ ->
                    Paired = lists:zipwith(fun(Slice, Info) ->
                        % Get the slice number
                        {_, _, Num} = filter_slice_value(Slice),

                        % Map the number to the info
                        {Num, bloomd:info_proplist(Info)}
                    end, Slices, InfoResults),

                    % Return the paired info
                    {ok, Paired}
            end
    end,

    % Repond
    {reply, Resp, State};


%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

% Handle a handoff request
handle_command(?FOLD_REQ{foldfun=Fun, acc0=Accum}, Sender, State=#state{idx=Idx}) ->
    {async, {handoff, Idx, Fun, Accum}, Sender, State};

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.


handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State#state{handoff=true}}.

handoff_cancelled(State) ->
    {ok, State#state{handoff=false}}.

handoff_finished(_TargetNode, State) ->
    {ok, State#state{handoff=false}}.

% Called to handle data that is received (receiver side)
handle_handoff_data(Data, State) ->
    Inp = br_handoff:decode(Data),
    Resp = br_handoff:handle_receive(Inp),
    {reply, Resp, State}.

% Called to encode data sender side
encode_handoff_item(Key, Value) ->
    br_handoff:encode(Key, Value).

% Checks if handoff is necessary. If is_empty then
% handoff immediately terminates.
is_empty(State) ->
    {true, State}.

% Called once handoff is done to delete all data
% owned by this vnode.
delete(State) ->
    {ok, State}.

% Normal handling of list_filters and info_filters
handle_coverage(Cmd=list_filters, _KeySpaces, Sender, State) ->
    handle_command(Cmd, Sender, State);
handle_coverage(Cmd={info_filter, _}, _KeySpaces, Sender, State) ->
    handle_command(Cmd, Sender, State);

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%
% Helper commands
%%%

% Issues a command against the local bloomd instance.
% This is an internal API, but is exposed so that
% we can use pmap to run commands in parallel to make
% use of command pipelining.
local_command(Elem, Cmd, State) ->
    case Cmd of
        {create, Options} -> bloomd:create(State#state.conn, Elem, Options);
        drop -> bloomd:drop(bloomd:filter(State#state.conn, Elem));
        close -> bloomd:close(bloomd:filter(State#state.conn, Elem));
        clear -> bloomd:clear(bloomd:filter(State#state.conn, Elem));
        flush -> bloomd:flush(bloomd:filter(State#state.conn, Elem));
        info -> bloomd:info(bloomd:filter(State#state.conn, Elem));
        _ -> {error, unknown_command}
    end.


% Returns the iolist representation of a given
% slice for a filter.
-spec filter_slice_name(integer(), iolist(), integer()) -> iolist().
filter_slice_name(Idx, FilterName, Slice) ->
    [integer_to_list(Idx), <<":">>, FilterName, <<":">>, integer_to_list(Slice)].

% Decomposes the filter into a tuple of the FilterName, index and slice
-spec filter_slice_value(iolist()) -> {binary(), integer(), integer()}.
filter_slice_value(Filter) when is_binary(Filter) ->
    % Find the offset of the colons
    Size = size(Filter),
    StartOffset = find_first(Filter, $:, 0, Size),
    EndOffset = find_last(Filter, $:, Size - 1),

    % Split
    Idx = binary:part(Filter, 0, StartOffset),
    Name = binary:part(Filter, StartOffset+1, EndOffset - StartOffset - 1),
    Num = binary:part(Filter, EndOffset+1, Size-EndOffset-1),

    % Convert to integer and return
    {Name, list_to_integer(binary_to_list(Idx)), list_to_integer(binary_to_list(Num))};

filter_slice_value(Filter) -> filter_slice_value(iolist_to_binary(Filter)).

% Finds the last occurrence of a character by
% searching a binary right-to-left.
find_last(_, _, -1) -> {error, not_found};
find_last(Bin, Char, Offset) ->
    case binary:at(Bin, Offset) of
        Char -> Offset;
        _ -> find_last(Bin, Char, Offset - 1)
    end.


% Finds the first occurrence of a character by
% searching a binary left-to-right
find_first(_, _, Offset, Len) when Offset =:= Len -> {error, not_found};
find_first(Bin, Char, Offset, Len) ->
    case binary:at(Bin, Offset) of
        Char -> Offset;
        _ -> find_first(Bin, Char, Offset + 1, Len)
    end.

% Checks if any of the commands returned an error
any_error(Results) -> lists:any(fun({error, _}) -> true; (_) -> false end, Results).

% Checks if any of the results has a given error type
has_error(Results, Errors) -> lists:any(fun({error, E}) -> lists:member(E, Errors); (_) -> false end, Results).

% Finds all the slices matching the given filter name in the local bloomd
% Returns a list of the slice names.
-spec matching_slices(binary(), #state{}) -> {error, command_failed} | [binary()].
matching_slices(FilterName, State) ->
    % Find all the filters
    Results = bloomd:list(State#state.conn),

    % Check for error
    case any_error(Results) of
        true -> {error, command_failed};
        _ ->
            % Find all the matching slices
            Parts = [{F, filter_slice_value(F)} || {F, _I} <- Results],
            [F || {F, {Name, Idx, _Slice}} <- Parts, Idx =:= State#state.idx, Name =:= FilterName]
    end.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

filter_slice_name_bin_test() ->
    Res = filter_slice_name(50, <<"testing">>, 0),
    <<"50:testing:0">> = iolist_to_binary(Res).

filter_slice_name_list_test() ->
    Res = filter_slice_name(21, ["foobar"], 128),
    <<"21:foobar:128">> = iolist_to_binary(Res).

filter_slice_value_bin_test() ->
    Res = filter_slice_value(<<"5:testing:123">>),
    {<<"testing">>, 5, 123} = Res.

filter_slice_value_list_test() ->
    Res = filter_slice_value(["100", ":", "foo:bar:baz",":", "64"]),
    {<<"foo:bar:baz">>, 100, 64} = Res.

find_last_bad_test() ->
    {error, not_found} = find_last(<<"hi there">>, $@, 7).

find_last_multi_test() ->
    Off = find_last(<<"a:b:c:d">>, $:, 6),
    Off = 5.

find_first_bad_test() ->
    {error, not_found} = find_first(<<"hi there">>, $@, 0, 7).

find_first_multi_test() ->
    Off = find_first(<<"a:b:c:d">>, $:, 0, 7),
    ?assertEqual(1, Off).

any_error_blank_test() ->
    false = any_error([]).

any_error_true_test() ->
    true = any_error([good, ok, {error, bad}]).

any_error_false_test() ->
    false = any_error([good, ok, tubez]).

has_error_blank_test() ->
    false = has_error([], [a,b,c]).

has_error_true_test() ->
    true = has_error([good, bad, {error, tubez}, {error, bad}], [bad]).

has_error_false_test() ->
    false = has_error([good, bad, {error, tubez}, {error, bad}], [notexist]).

-endif.


