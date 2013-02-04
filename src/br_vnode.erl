-module(br_vnode).
-behaviour(riak_core_vnode).
-include("bloomd_ring.hrl").

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

        % Connection to local bloomd
        conn
        }).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, LocalHost} = application:get_env(bloomd_ring, bloomd_local_host),
    {ok, LocalPort} = application:get_env(bloomd_ring, bloomd_local_port),

    % Try to connect to the local bloomd
    Conn = bloomd:new(LocalHost, LocalPort),

    % Setup our state
    {ok, #state {partition=Partition, conn=Conn}}.


%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};


%%%
% Handles the create filter command. This command takes the slices
% to create, since a particular v-node may be responsible for multiple
% slices. It also takes an options list that should be passed down.
%
% We should respond with one of done, exists, or {error, command_failed}
%%%
handle_command({create_filter, FilterName, Slices, Options}, _Sender, State) ->
    % Convert into the proper names
    Names = [filter_slice_name(FilterName, S) || S <- Slices],

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
                _ -> done
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
            FilterInfoList = [{Name, {Slice, bloomd:filter_info(I)}} || {F, I} <- Results,
                                                        {Name, Slice} = filter_slice_value(F)],

            % Collapse the duplicates by name
            FilterCombined = lists:foldl(fun({Name, Info}, Accum) ->
                dict:append(Name, Info, Accum)
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
    Resp = case matching_slices(FilterName, State) of
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
    end,

    % Repond
    {reply, Resp, State};


handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

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
        _ -> {error, unknown_command}
    end.


% Returns the iolist representation of a given
% slice for a filter.
-spec filter_slice_name(iolist(), integer()) -> iolist().
filter_slice_name(FilterName, Slice) -> [FilterName, <<":">>, integer_to_list(Slice)].

% Decomposes the filter into a tuple of the FilterName and slice
-spec filter_slice_value(iolist()) -> {binary(), integer()}.
filter_slice_value(Filter) when is_binary(Filter) ->
    % Find the offset of the colon
    Offset = find_last(Filter, $:, size(Filter) - 1),

    % Split
    Name = binary:part(Filter, 0, Offset),
    Num = binary:part(Filter, Offset+1),

    % Convert to integer and return
    {Name, list_to_integer(binary_to_list(Num))};

filter_slice_value(Filter) -> filter_slice_value(iolist_to_binary(Filter)).

% Finds the last occurrence of a character by
% searching a binary right-to-left.
find_last(_, _, -1) -> {error, not_found};
find_last(Bin, Char, Offset) ->
    case binary:at(Bin, Offset) of
        Char -> Offset;
        _ -> find_last(Bin, Char, Offset - 1)
    end.

% Checks if any of the commands returned an error
any_error(Results) -> lists:any(fun({error, _}) -> true; (_) -> false end, Results).

% Checks if any of the results has a given error type
has_error(Results, Errors) -> lists:any(fun({error, E}) -> lists:contains(E, Errors); (_) -> false end, Results).

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
            [F || {F, _I} <- Results,
                {Name, _Slice} = filter_slice_value(F),
                  Name =:= FilterName]
    end.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

filter_slice_name_bin_test() ->
    Res = filter_slice_name(<<"testing">>, 0),
    <<"testing:0">> = iolist_to_binary(Res).

filter_slice_name_list_test() ->
    Res = filter_slice_name(["foobar"], 128),
    <<"foobar:128">> = iolist_to_binary(Res).

filter_slice_value_bin_test() ->
    Res = filter_slice_value(<<"testing:123">>),
    {<<"testing">>, 123} = Res.

filter_slice_value_list_test() ->
    Res = filter_slice_value(["foo:bar:baz",":", "64"]),
    {<<"foo:bar:baz">>, 64} = Res.

find_last_bad_test() ->
    {error, not_found} = find_last(<<"hi there">>, $@, 7).

find_last_multi_test() ->
    Off = find_last(<<"a:b:c:d">>, $:, 6).
    Off = 5.

any_error_blank_test() ->
    false = any_error([]).

any_error_true_test() ->
    true = any_error([good, ok, {error, bad}]).

any_error_false_test() ->
    false = any_error([good, ok, tubez]).

has_error_blank_test() ->
    false = hash_error([], [a,b,c]).

has_error_true_test() ->
    true = hash_error([good, bad, {error, tubez}, {error, bad}], [bad]).

has_error_false_test() ->
    false = hash_error([good, bad, {error, tubez}, {error, bad}], [notexist]).

-endif.


