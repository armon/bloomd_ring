-module(br_ascii_handler).
-behavior(gen_server).
-export([start_link/1, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
            socket,
            buffer=[]
        }).


-define(NEWLINE, <<"\n">>).
-define(DONE, <<"Done\n">>).
-define(START, <<"START\n">>).
-define(END, <<"END\n">>).
-define(SPACE, <<" ">>).
-define(CLIENT_ERR, <<"Client Error: ">>).
-define(CMD_NOT_SUP, <<"Command not supported">>).
-define(BAD_ARGS, <<"Bad arguments">>).
-define(UNEXPECTED_ARGS, <<"Unexpected arguments">>).
-define(FILT_KEY_NEEDED, <<"Must provide filter name and key">>).
-define(FILT_NEEDED, <<"Must provide filter name">>).
-define(BAD_FILT_NAME, <<"Bad filter name">>).
-define(INTERNAL_ERR, <<"Internal Error\n">>).
-define(FILT_NOT_EXIST, <<"Filter does not exist\n">>).
-define(FILT_NOT_PROXIED, <<"Filter is not proxied. Close it first.\n">>).
-define(EXISTS, <<"Exists\n">>).
-define(YES_SPACE, <<"Yes ">>).
-define(NO_SPACE, <<"No ">>).
-define(YES_RESP, <<"Yes\n">>).
-define(NO_RESP, <<"No\n">>).
-define(VALID_FILT_RE, "^[^\s]{1,200}$").


-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

start_link(Socket) ->
    gen_server:start_link(?MODULE, [Socket], []).


init([Socket]) ->
    State = #state{socket=Socket},
    {ok, State}.


handle_call(_Msg, _From, State) ->
    {reply, ok, State}.


% Wait for the start message, set the socket to active
handle_cast(start, S=#state{socket=Socket}) ->
    inet:setopts(Socket, [{active, true}]),
    {noreply, S}.


% Store new data in the buffer
handle_info({tcp, _, Data}, State=#state{buffer=Buf}) ->
    NewBuf = iolist_to_binary([Buf, Data]),
    NS = process_buffer(State, NewBuf),
    {noreply, NS};


% Handle a close
handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};


% Handle an error
handle_info({tcp_error, _Socket, Reason}, State) ->
    lager:warning("TCP Error on socket ~p. Err: ~p", [State, Reason]),
    {stop, normal, State};


% Ignore unexpected messages
handle_info(Msg, State) ->
    lager:warning("Got unexpected message: ~p", [Msg]),
    {noreply, State}.


terminate(_Reason, _State) -> ok.
code_change(_OldVsvn, State, _Extra) -> {ok, State}.

%%%
% Request processing
%%%

% Processes all commands in a buffer
process_buffer(State, Buffer) ->
    case binary:split(Buffer, [<<"\r\n">>, <<"\n">>]) of
        % No further commands can be processed, return remaining buffer
        [_] -> State#state{buffer=Buffer};

        % Process each available command
        [Cmd, Buf] ->
            S1 = process_cmd(State, Cmd),
            process_buffer(S1, Buf)
    end.

%%%
% process_cmd inspects the line to determine which command is
% being invoked, parses the arguments and formats the responses.
%%%

process_cmd(State, <<"c ", Rest/binary>>) ->
    process_check_set(check, State, Rest);
process_cmd(State, <<"check ", Rest/binary>>) ->
    process_check_set(check, State, Rest);

process_cmd(State, <<"m ", Rest/binary>>) ->
    process_multi_bulk(multi, State, Rest);
process_cmd(State, <<"multi ", Rest/binary>>) ->
    process_multi_bulk(multi, State, Rest);


process_cmd(State, <<"s ", Rest/binary>>) ->
    process_check_set(set, State, Rest);
process_cmd(State, <<"set ", Rest/binary>>) ->
    process_check_set(set, State, Rest);

process_cmd(State, <<"b ", Rest/binary>>) ->
    process_multi_bulk(bulk, State, Rest);
process_cmd(State, <<"bulk ", Rest/binary>>) ->
    process_multi_bulk(bulk, State, Rest);

process_cmd(State, <<"info ", Rest/binary>>) ->
    % Get the filter and if absolute mode is one
    ParseResult = case split(Rest, ?SPACE, true) of
        % Handle the +absolute extension case
        [Filter, Modifier] when Modifier =:= <<"+absolute">> ->
            {Filter, true};

        % Handle standard info case
        [Filter] -> {Filter, false};

        % Handle the no filter case
        [] -> {error, need_filter};

        % Handle the too many args case
        _ -> {error, unexpected_args}
    end,

    % Determine the result of the call
    Result = case ParseResult of
        {error, ErrType} -> {error, ErrType};
        {F, Abs} ->
            case valid_filter(F) of
                true -> bloomd_ring:info(F, Abs);
                _ -> {error, bad_filter}
           end
    end,

    % Respond to the client
    case Result of
        {ok, Props} ->
            % Format the response block
            Formatted = lists:map(fun({Prop, Val}) ->
                V = case Prop of
                    probability -> format_float(Val);
                    _ -> integer_to_list(Val)
                end,
                [atom_to_list(Prop), ?SPACE, V]
            end, Props),

            % Send the response
            send_list(State#state.socket, Formatted);

        {error, need_filter} ->
            gen_tcp:send(State#state.socket,
                         [?CLIENT_ERR, ?FILT_NEEDED, ?NEWLINE]);
        {error, unexpected_args} ->
            gen_tcp:send(State#state.socket,
                         [?CLIENT_ERR, ?UNEXPECTED_ARGS, ?NEWLINE]);
        {error, bad_filter} ->
            gen_tcp:send(State#state.socket,
                         [?CLIENT_ERR, ?BAD_FILT_NAME, ?NEWLINE]);
        {error, no_filter} ->
            gen_tcp:send(State#state.socket, [?FILT_NOT_EXIST]);
        {error, _} ->
            gen_tcp:send(State#state.socket, [?INTERNAL_ERR])
    end,
    State;

process_cmd(State, <<"drop ", Rest/binary>>) ->
    filter_needed(fun(Filter) ->
        Result = bloomd_ring:drop(Filter),
        case Result of
            {ok, done} ->
                gen_tcp:send(State#state.socket, [?DONE]);
            {error, no_filter} ->
                gen_tcp:send(State#state.socket, [?FILT_NOT_EXIST]);
            {error, _} ->
                gen_tcp:send(State#state.socket, [?INTERNAL_ERR])
        end,
        State
    end, Rest, State);

process_cmd(State, <<"close ", Rest/binary>>) ->
    filter_needed(fun(Filter) ->
        Result = bloomd_ring:close(Filter),
        case Result of
            {ok, done} ->
                gen_tcp:send(State#state.socket, [?DONE]);
            {error, no_filter} ->
                gen_tcp:send(State#state.socket, [?FILT_NOT_EXIST]);
            {error, _} ->
                gen_tcp:send(State#state.socket, [?INTERNAL_ERR])
        end,
        State
    end, Rest, State);

process_cmd(State, <<"clear ", Rest/binary>>) ->
    filter_needed(fun(Filter) ->
        Result = bloomd_ring:clear(Filter),
        case Result of
            {ok, done} ->
                gen_tcp:send(State#state.socket, [?DONE]);
            {error, no_filter} ->
                gen_tcp:send(State#state.socket, [?FILT_NOT_EXIST]);
            {error, not_proxied} ->
                gen_tcp:send(State#state.socket, [?FILT_NOT_PROXIED]);
            {error, _} ->
                gen_tcp:send(State#state.socket, [?INTERNAL_ERR])
        end,
        State
    end, Rest, State);

process_cmd(State, <<"create ", Rest/binary>>) ->
    case split(Rest, ?SPACE, true) of
        [Filter | Options] ->
            case valid_filter(Filter) of
                true ->
                    case parse_create_options(Options) of
                        {error, badargs} ->
                            gen_tcp:send(State#state.socket,
                                 [?CLIENT_ERR, ?BAD_ARGS, ?NEWLINE]);

                        Opts ->
                            Result = bloomd_ring:create(Filter, Opts),
                            case Result of
                                {ok, exists} ->
                                    gen_tcp:send(State#state.socket, [?EXISTS]);
                                {ok, done} ->
                                    gen_tcp:send(State#state.socket, [?DONE]);
                                {error, {client_error, Err}} ->
                                    gen_tcp:send(State#state.socket,
                                                 [?CLIENT_ERR, Err, ?NEWLINE]);
                                {error, _} ->
                                    gen_tcp:send(State#state.socket, [?INTERNAL_ERR])
                            end
                    end;

                _ ->
                    gen_tcp:send(State#state.socket,
                                 [?CLIENT_ERR, ?BAD_FILT_NAME, ?NEWLINE])
            end;

        % Handle the no filter case
        [] ->
            gen_tcp:send(State#state.socket,
                         [?CLIENT_ERR, ?FILT_NEEDED, ?NEWLINE])
    end,
    State;


process_cmd(State, <<"list", Rest/binary>>) ->
    Result = case split(Rest, ?SPACE, true) of
        % Handle the +absolute extension case
        [Modifier] when Modifier =:= <<"+absolute">> ->
            bloomd_ring:list(true);
        % Handle standard case
        [] -> bloomd_ring:list(false);
        % Too many args
        _ -> {error, unexpected_args}
    end,
    case Result of
        {error, unexpected_args} ->
            gen_tcp:send(State#state.socket,
                         [?CLIENT_ERR, ?UNEXPECTED_ARGS, ?NEWLINE]);
        {error, _} ->
            gen_tcp:send(State#state.socket, [?INTERNAL_ERR]);

        {ok, Results} ->
            % Format the results
            Formatted = lists:map(fun({Filter, Info}) ->
                Prob      = proplists:get_value(probability, Info, 0),
                Bytes     = proplists:get_value(bytes, Info, 0),
                Capacity  = proplists:get_value(capacity, Info, 0),
                Size      = proplists:get_value(size, Info, 0),
                [Filter, ?SPACE,
                        format_float(Prob), ?SPACE,
                        integer_to_list(Bytes), ?SPACE,
                        integer_to_list(Capacity), ?SPACE,
                        integer_to_list(Size)]
            end, Results),
            send_list(State#state.socket, Formatted)
    end,
    State;


% Handle the filter vs no-filter case
process_cmd(State, <<"flush ", Rest/binary>>) ->
    filter_needed(fun(Filter) ->
        Result = bloomd_ring:flush(Filter),
        case Result of
            {ok, done} ->
                gen_tcp:send(State#state.socket, [?DONE]);
            {error, no_filter} ->
                gen_tcp:send(State#state.socket, [?FILT_NOT_EXIST]);
            {error, _} ->
                gen_tcp:send(State#state.socket, [?INTERNAL_ERR])
        end,
        State
    end, Rest, State);

process_cmd(State, <<"flush", Rest/binary>>) ->
    no_args_needed(fun() ->
        Result = bloomd_ring:flush(undefined),
        case Result of
            {ok, done} ->
                gen_tcp:send(State#state.socket, [?DONE]);
            {error, no_filter} ->
                gen_tcp:send(State#state.socket, [?FILT_NOT_EXIST]);
            {error, _} ->
                gen_tcp:send(State#state.socket, [?INTERNAL_ERR])
        end,
        State
    end, Rest, State);

% Catch all for an undefined command
process_cmd(State=#state{socket=Sock}, Cmd) ->
    % Check if this is a command that takes an argument, and is simply missing
    % any arguments
    Requires = case Cmd of
        <<"c">>      -> filter_key;
        <<"check">>  -> filter_key;
        <<"m">>      -> filter_key;
        <<"multi">>  -> filter_key;
        <<"s">>      -> filter_key;
        <<"set">>    -> filter_key;
        <<"b">>      -> filter_key;
        <<"bulk">>   -> filter_key;
        <<"info">>   -> filter;
        <<"drop">>   -> filter;
        <<"close">>  -> filter;
        <<"clear">>  -> filter;
        <<"create">> -> filter;
        _            -> unknown
    end,

    % Get the appropriate message
    Msg = case Requires of
        filter_key -> ?FILT_KEY_NEEDED;
        filter -> ?FILT_NEEDED;
        unknown -> ?CMD_NOT_SUP
    end,

    % Send and return
    gen_tcp:send(Sock, [?CLIENT_ERR, Msg, ?NEWLINE]), State.

%%%
% Shared command processors, for re-use if a command
% supports aliasing
%%%

process_check_set(Op, State, Rest) ->
    filter_key_needed(fun(Filter, [Key]) ->
        Result = bloomd_ring:Op(Filter, Key),
        case Result of
            {ok, false} ->
                gen_tcp:send(State#state.socket, [?NO_RESP]);
            {ok, true} ->
                gen_tcp:send(State#state.socket, [?YES_RESP]);
            {error, no_filter} ->
                gen_tcp:send(State#state.socket, [?FILT_NOT_EXIST]);
            {error, _} ->
                gen_tcp:send(State#state.socket, [?INTERNAL_ERR])
        end,
        State
    end, Rest, State).


process_multi_bulk(Op, State, Rest) ->
    filter_keys_needed(fun(Filter, Keys) ->
        Result = bloomd_ring:Op(Filter, Keys),
        case Result of
            {ok, Vals} ->
                {Formatted, _} = lists:foldl(fun(V, {Resp, Idx}) ->
                    Res = case Idx of
                        0 ->
                            case V of
                                true -> ?YES_RESP;
                                _ -> ?NO_RESP
                            end;
                        _ ->
                            case V of
                                true -> ?YES_SPACE;
                                _ -> ?NO_SPACE
                            end
                    end,
                    {[Res | Resp], Idx+1}
                end, {[], 0}, lists:reverse(Vals)),
                gen_tcp:send(State#state.socket, [Formatted]);
            {error, no_filter} ->
                gen_tcp:send(State#state.socket, [?FILT_NOT_EXIST]);
            {error, _} ->
                gen_tcp:send(State#state.socket, [?INTERNAL_ERR])
        end,
        State
    end, Rest, State).


%%%
% Helpers
%%%

% This helper method checks that a filter name is provided
% as well as key arguments. If this condition is not met,
% then a client error is generated. Otherwise, the provided
% function of arity 2 is invoked with the filter and key(s).
filter_key_needed(Func, Remain, State) ->
    filter_keys_needed(Func, Remain, State, true).
filter_keys_needed(Func, Remain, State) ->
    filter_keys_needed(Func, Remain, State, false).

filter_keys_needed(Func, Remain, State, SingleKey) ->
    case split(Remain, ?SPACE, true) of
        [Filter, Key | Keys] ->
            % Validate the filter
            case valid_filter(Filter) of
                true ->
                    % Handle the case of single key required
                    case Keys of
                        [_First | _Tail] when SingleKey ->
                            gen_tcp:send(State#state.socket,
                                 [?CLIENT_ERR, ?UNEXPECTED_ARGS, ?NEWLINE]),
                            State;

                        _ -> Func(Filter, [Key | Keys])
                    end;

                % Handle bad filter names
                _ ->
                    gen_tcp:send(State#state.socket,
                                 [?CLIENT_ERR, ?BAD_FILT_NAME, ?NEWLINE]),
                    State
            end;

        % Ensure we have a filter and at least one key
        _ ->
            gen_tcp:send(State#state.socket,
                         [?CLIENT_ERR, ?FILT_KEY_NEEDED, ?NEWLINE]),
            State
    end.


% This helper ensures that a filter is provided, and
% has a valid name. The appropriate error codes are returned
% if necessary, otherwise a callback function of arity 1
% is invoked with the filter.
filter_needed(Func, Remain, State) ->
    case split(Remain, ?SPACE, true) of
        % Ensure we have a filter only
        [Filter] ->
            % Validate the filter
            case valid_filter(Filter) of
                true -> Func(Filter);

                % Handle bad filter names
                _ ->
                    gen_tcp:send(State#state.socket,
                                 [?CLIENT_ERR, ?BAD_FILT_NAME, ?NEWLINE]),
                    State
            end;

        [] ->
            gen_tcp:send(State#state.socket,
                         [?CLIENT_ERR, ?FILT_NEEDED, ?NEWLINE]),
            State;

        _ ->
            gen_tcp:send(State#state.socket,
                         [?CLIENT_ERR, ?UNEXPECTED_ARGS, ?NEWLINE]),
            State
    end.


% This helper ensures a command is called with no further arguments.
% It then invokes a callback of arity 0, or provides the appropriate
% error tot he user.
no_args_needed(Func, Remain, State) ->
    case Remain of
        <<>> -> Func();
        _ ->
            gen_tcp:send(State#state.socket,
                         [?CLIENT_ERR, ?UNEXPECTED_ARGS, ?NEWLINE]),
            State
    end.


% Checks if a filter name is valid
valid_filter(Filter) ->
    % Get the cached regex
    Re = case get(filter_re) of
        undefined ->
            {ok, R} = re:compile(?VALID_FILT_RE),
            put(filter_re, R),
            R;
        X -> X
    end,

    % Check for a match
    case re:run(Filter, Re) of
        {match, _} -> true;
        _ -> false
    end.


% Parses the options list for the connect command
% into a proplist.
-spec parse_create_options([binary()]) -> {error, badargs} | [{atom(), term()}].
parse_create_options(Options) ->
    parse_create_options(Options, []).

% Tail recursive helper
parse_create_options([], Props) -> Props;
parse_create_options([Opt | Remain], Props) ->
    % Split on the equals sign
    case split(Opt, <<"=">>, false) of
        % Should be name=val
        [Name, Raw] ->
            KeyRaw = list_to_atom(binary_to_list(Name)),

            % Re-write the key if necessary
            Key = case KeyRaw of
                prob -> probability;
                K -> K
            end,

            % Convert based on the name
            Val = case Key of
                capacity    -> to_integer(Raw);
                probability -> to_float(Raw);
                in_memory   -> to_integer(Raw);
                _           -> error
            end,

            % Validate the conversion
            case Val of
                error -> {error, badargs};

                % Recursively handle the other options
                _ -> parse_create_options(Remain, [{Key, Val} | Props])
            end;

        % Bad arg
        _ -> {error, badargs}
    end.


% Does a split that filters out empty binaries
split(Bin, Patterns, Global) ->
    % Get the options
    Opts = case Global of
        true -> [global];
        _ -> []
    end,

    % Do the split
    Res = binary:split(Bin, Patterns, Opts),

    % Filter
    [B || B <- Res, B =/= <<>>].


% Sends a list oriented response as
% START
% N1
% N2
% ..
% END
send_list(Sock, List) ->
    % Terminate each line
    Terminated = [[Line, ?NEWLINE] || Line <- List],
    gen_tcp:send(Sock, [?START, Terminated, ?END]).


-spec to_float(binary()) -> error | float().
to_float(Bin) ->
    try list_to_float(binary_to_list(Bin))
    catch
        error:_ -> case to_integer(Bin) of
            error -> error;
            Int -> float(Int)
        end
    end.


-spec to_integer(binary()) -> error | integer().
to_integer(Bin) ->
    try list_to_integer(binary_to_list(Bin))
    catch
        error:_ -> error
    end.


% Gives a nice base 10 representation of a float
format_float(Val) ->
    % Get the whole number part
    WholePart = trunc(Val),

    % Get the sub part
    SubPart = abs(trunc(Val * 1000000)) rem 1000000,

    % Convert to iolist
    SubL = integer_to_list(SubPart),
    Pad = ["0" || _X <- lists:seq(1, 6 - length(SubL))],
    [integer_to_list(WholePart), ".", Pad, SubL].



-ifdef(TEST).

to_float_test() ->
    ?assertEqual(error, to_float(<<"tubez0">>)),
    ?assertEqual(1.0, to_float(<<"1.0">>)),
    ?assertEqual(1.0, to_float(<<"1">>)),
    ?assertEqual(-1.0, to_float(<<"-1">>)).

to_int_test() ->
    ?assertEqual(error, to_integer(<<"junk">>)),
    ?assertEqual(1, to_integer(<<"1">>)),
    ?assertEqual(-1, to_integer(<<"-1">>)).

format_float_test() ->
    ?assertEqual(["-123", ".", [], "123456"], format_float(-123.123456)),
    ?assertEqual(["123", ".", [], "123456"], format_float(123.123456)),
    ?assertEqual(["0", ".", ["0", "0"], "1000"], format_float(0.001)).

-endif.
