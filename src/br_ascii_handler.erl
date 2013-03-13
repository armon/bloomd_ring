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
-define(VALID_FILT_RE, "^[^ \t\n\r]{1,200}$").

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

start_link(Socket) ->
    gen_server:start_link(?MODULE, [Socket], []).


init([Socket]) ->
    State = #state{socket=Socket},
    {ok, State}.


handle_call(Msg, _From, State) ->
    lager:warning("Got unexpected command: ~p", [Msg]),
    {reply, ok, State}.


% Wait for the start message, set the socket to active
handle_cast(start, S=#state{socket=Socket}) ->
    inet:setopts(Socket, [{active, once}, {nodelay, true}]),
    {noreply, S}.


% Store new data in the buffer
handle_info({tcp, S, Data}, State=#state{buffer=Buf}) ->
    NewBuf = iolist_to_binary([Buf, Data]),
    NS = process_buffer(State, NewBuf),
    inet:setopts(S, [{active, once}]),
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
        % Process each available command
        [Cmd, Buf] ->
            S1 = process_cmd(State, Cmd),
            process_buffer(S1, Buf);

        % No further commands can be processed, return remaining buffer
        _ -> State#state{buffer=Buffer}
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
            {error, _} ->
                gen_tcp:send(State#state.socket, [?INTERNAL_ERR])
        end,
        State
    end, Rest, State);

% Ignore a blank line
process_cmd(State, <<>>) ->
    State;

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
                gen_tcp:send(State#state.socket, Formatted);
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

                % Stop if probability is invalid
                V when V >= 1.0 orelse V =< 0, Key =:= probability ->
                    {error, badargs};

                V when V =< 0, Key =:= capacity ->
                    {error, badargs};

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

send_list_test() ->
    Expect = [<<"START\n">>, [[<<"tubez">>, <<"\n">>]], <<"END\n">>],
    M = em:new(),
    em:strict(M, gen_tcp, send, [undefined, Expect], {return, ok}),
    ok = em:replay(M),

    send_list(undefined, [<<"tubez">>]),
    em:verify(M).

split_non_global_test() ->
    In = <<"test a string">>,
    ?assertEqual([<<"test">>, <<"a string">>], split(In, ?SPACE, false)).

split_global_test() ->
    In = <<"test a string">>,
    ?assertEqual([<<"test">>, <<"a">>, <<"string">>], split(In, ?SPACE, true)).

split_drop_blank_test() ->
    In = <<"test a          string">>,
    ?assertEqual([<<"test">>, <<"a">>, <<"string">>], split(In, ?SPACE, true)).

parse_create_invalid_test() ->
    Inp = [<<"capacity=10000">>, <<"bad=1">>],
    ?assertEqual({error, badargs}, parse_create_options(Inp)),
    ?assertEqual({error, badargs}, parse_create_options([<<"foobar">>])),
    ?assertEqual({error, badargs}, parse_create_options([<<"capacity=foo">>])),
    ?assertEqual({error, badargs}, parse_create_options([<<"capacity=0">>])),
    ?assertEqual({error, badargs}, parse_create_options([<<"capacity=-100">>])),
    ?assertEqual({error, badargs}, parse_create_options([<<"probability=0">>])),
    ?assertEqual({error, badargs}, parse_create_options([<<"probability=1">>])),
    ?assertEqual({error, badargs}, parse_create_options([<<"probability=-0.5">>])),
    ?assertEqual({error, badargs}, parse_create_options([<<"probability=foo">>])),
    ?assertEqual({error, badargs}, parse_create_options([<<"prob=foo">>])),
    ?assertEqual({error, badargs}, parse_create_options([<<"in_memory=foo">>])),
    ?assertEqual({error, badargs}, parse_create_options([<<"in_memory=2.0">>])).

parse_create_valid_test() ->
    Inp = [<<"capacity=10000">>, <<"in_memory=0">>, <<"prob=0.001">>],
    Out = [{probability, 0.001}, {in_memory, 0}, {capacity, 10000}],
    ?assertEqual(Out, parse_create_options(Inp)).

invalid_filter_test() ->
    ?assertEqual(false, valid_filter(<<"">>)),
    ?assertEqual(false, valid_filter(<<"    ">>)),
    ?assertEqual(false, valid_filter(<<"\t">>)),
    ?assertEqual(false, valid_filter(<<"\n">>)).

valid_filter_test() ->
    ?assertEqual(true, valid_filter(<<"a">>)),
    ?assertEqual(true, valid_filter(<<"foo:bar:baz">>)),
    ?assertEqual(true, valid_filter(<<"!@#$%^&*()_+-=">>)),
    ?assertEqual(true, valid_filter(<<"Unicøde">>)).

invalid_no_args_needed_test() ->
    Expect = [?CLIENT_ERR, ?UNEXPECTED_ARGS, ?NEWLINE],
    M = em:new(),
    em:strict(M, gen_tcp, send, [undefined, Expect], {return, ok}),
    ok = em:replay(M),

    S = #state{socket=undefined},
    Valid = fun() -> true end,
    ?assertEqual(S, no_args_needed(Valid, <<"more">>, S)),
    em:verify(M).

valid_no_args_needed_test() ->
    Valid = fun() -> true end,
    ?assertEqual(true, no_args_needed(Valid, <<>>, false)).

valid_filter_needed_test() ->
    Valid = fun(<<"tubez">>) -> true end,
    ?assertEqual(true, filter_needed(Valid, <<"tubez">>, false)).

missing_filter_needed_test() ->
    Expect = [?CLIENT_ERR, ?FILT_NEEDED, ?NEWLINE],
    M = em:new(),
    em:strict(M, gen_tcp, send, [undefined, Expect], {return, ok}),
    ok = em:replay(M),


    S = #state{socket=undefined},
    ?assertEqual(S, filter_needed(undefined, <<>>, S)),
    em:verify(M).

extra_filter_needed_test() ->
    Expect = [?CLIENT_ERR, ?UNEXPECTED_ARGS, ?NEWLINE],
    M = em:new(),
    em:strict(M, gen_tcp, send, [undefined, Expect], {return, ok}),
    ok = em:replay(M),


    S = #state{socket=undefined},
    ?assertEqual(S, filter_needed(undefined, <<"tubez more">>, S)),
    em:verify(M).

invalid_filter_needed_test() ->
    Expect = [?CLIENT_ERR, ?BAD_FILT_NAME, ?NEWLINE],
    M = em:new(),
    em:strict(M, gen_tcp, send, [undefined, Expect], {return, ok}),
    ok = em:replay(M),


    S = #state{socket=undefined},
    ?assertEqual(S, filter_needed(undefined, <<"\t">>, S)),
    em:verify(M).

valid_filter_key_needed_test() ->
    Valid = fun(<<"filter">>, [<<"key">>]) -> true end,
    ?assertEqual(true, filter_key_needed(Valid, <<"filter key">>, false)).

toomany_filter_key_needed_test() ->
    Expect = [?CLIENT_ERR, ?UNEXPECTED_ARGS, ?NEWLINE],
    M = em:new(),
    em:strict(M, gen_tcp, send, [undefined, Expect], {return, ok}),
    ok = em:replay(M),

    S = #state{socket=undefined},
    ?assertEqual(S, filter_key_needed(undefined, <<"filter key key2">>, S)),
    em:verify(M).

missing_filter_key_needed_test() ->
    Expect = [?CLIENT_ERR, ?FILT_KEY_NEEDED, ?NEWLINE],
    M = em:new(),
    em:strict(M, gen_tcp, send, [undefined, Expect], {return, ok}),
    ok = em:replay(M),

    S = #state{socket=undefined},
    ?assertEqual(S, filter_key_needed(undefined, <<"">>, S)),
    em:verify(M).

bad_filter_key_needed_test() ->
    Expect = [?CLIENT_ERR, ?BAD_FILT_NAME, ?NEWLINE],
    M = em:new(),
    em:strict(M, gen_tcp, send, [undefined, Expect], {return, ok}),
    ok = em:replay(M),

    S = #state{socket=undefined},
    ?assertEqual(S, filter_key_needed(undefined, <<"\t key">>, S)),
    em:verify(M).

valid_filter_keys_needed_test() ->
    Valid = fun(<<"filter">>, [<<"key">>, <<"key2">>]) -> true end,
    ?assertEqual(true, filter_keys_needed(Valid, <<"filter key key2">>, false)).

valid_process_multi_bulk_test() ->
    Expect = [?YES_SPACE, ?NO_SPACE, ?YES_RESP],
    M = em:new(),
    em:strict(M, bloomd_ring, multi, [<<"F">>, [<<"A">>, <<"B">>, <<"C">>]],
                            {return, {ok, [true, false, true]}}),
    em:strict(M, gen_tcp, send, [undefined, Expect], {return, ok}),
    ok = em:replay(M),


    S = #state{socket=undefined},
    ?assertEqual(S, process_multi_bulk(multi, S, <<"F A B C">>)),
    em:verify(M).

single_process_multi_bulk_test() ->
    Expect = [?NO_RESP],
    M = em:new(),
    em:strict(M, bloomd_ring, bulk, [<<"F">>, [<<"A">>]],
                            {return, {ok, [false]}}),
    em:strict(M, gen_tcp, send, [undefined, Expect], {return, ok}),
    ok = em:replay(M),


    S = #state{socket=undefined},
    ?assertEqual(S, process_multi_bulk(bulk, S, <<"F A">>)),
    em:verify(M).

nofilt_process_multi_bulk_test() ->
    Expect = [?FILT_NOT_EXIST],
    M = em:new(),
    em:strict(M, bloomd_ring, bulk, [<<"F">>, [<<"A">>]],
                            {return, {error, no_filter}}),
    em:strict(M, gen_tcp, send, [undefined, Expect], {return, ok}),
    ok = em:replay(M),

    S = #state{socket=undefined},
    ?assertEqual(S, process_multi_bulk(bulk, S, <<"F A">>)),
    em:verify(M).

failed_process_multi_bulk_test() ->
    Expect = [?INTERNAL_ERR],
    M = em:new(),
    em:strict(M, bloomd_ring, bulk, [<<"F">>, [<<"A">>]],
                            {return, {error, timeout}}),
    em:strict(M, gen_tcp, send, [undefined, Expect], {return, ok}),
    ok = em:replay(M),

    S = #state{socket=undefined},
    ?assertEqual(S, process_multi_bulk(bulk, S, <<"F A">>)),
    em:verify(M).

valid_process_check_set_test() ->
    Expect = [?NO_RESP],
    M = em:new(),
    em:strict(M, bloomd_ring, set, [<<"F">>, <<"A">>],
                            {return, {ok, false}}),
    em:strict(M, gen_tcp, send, [undefined, Expect], {return, ok}),
    ok = em:replay(M),


    S = #state{socket=undefined},
    ?assertEqual(S, process_check_set(set, S, <<"F A">>)),
    em:verify(M).

yes_valid_process_check_set_test() ->
    Expect = [?YES_RESP],
    M = em:new(),
    em:strict(M, bloomd_ring, set, [<<"F">>, <<"A">>],
                            {return, {ok, true}}),
    em:strict(M, gen_tcp, send, [undefined, Expect], {return, ok}),
    ok = em:replay(M),


    S = #state{socket=undefined},
    ?assertEqual(S, process_check_set(set, S, <<"F A">>)),
    em:verify(M).

nofilt_process_check_set_test() ->
    Expect = [?FILT_NOT_EXIST],
    M = em:new(),
    em:strict(M, bloomd_ring, check, [<<"F">>, <<"A">>],
                            {return, {error, no_filter}}),
    em:strict(M, gen_tcp, send, [undefined, Expect], {return, ok}),
    ok = em:replay(M),

    S = #state{socket=undefined},
    ?assertEqual(S, process_check_set(check, S, <<"F A">>)),
    em:verify(M).

failed_process_check_set_test() ->
    Expect = [?INTERNAL_ERR],
    M = em:new(),
    em:strict(M, bloomd_ring, set, [<<"F">>, <<"A">>],
                            {return, {error, timeout}}),
    em:strict(M, gen_tcp, send, [undefined, Expect], {return, ok}),
    ok = em:replay(M),

    S = #state{socket=undefined},
    ?assertEqual(S, process_check_set(set, S, <<"F A">>)),
    em:verify(M).

invalid_process_cmd_test() ->
    NotSupExpect = [?CLIENT_ERR, ?CMD_NOT_SUP, ?NEWLINE],
    FiltExp = [?CLIENT_ERR, ?FILT_NEEDED, ?NEWLINE],
    FiltKeyExp = [?CLIENT_ERR, ?FILT_KEY_NEEDED, ?NEWLINE],
    M = em:new(),
    em:strict(M, gen_tcp, send, [undefined, FiltKeyExp]),
    em:strict(M, gen_tcp, send, [undefined, FiltKeyExp]),
    em:strict(M, gen_tcp, send, [undefined, FiltKeyExp]),
    em:strict(M, gen_tcp, send, [undefined, FiltKeyExp]),
    em:strict(M, gen_tcp, send, [undefined, FiltKeyExp]),
    em:strict(M, gen_tcp, send, [undefined, FiltKeyExp]),
    em:strict(M, gen_tcp, send, [undefined, FiltKeyExp]),
    em:strict(M, gen_tcp, send, [undefined, FiltKeyExp]),

    em:strict(M, gen_tcp, send, [undefined, FiltExp]),
    em:strict(M, gen_tcp, send, [undefined, FiltExp]),
    em:strict(M, gen_tcp, send, [undefined, FiltExp]),
    em:strict(M, gen_tcp, send, [undefined, FiltExp]),
    em:strict(M, gen_tcp, send, [undefined, FiltExp]),

    em:strict(M, gen_tcp, send, [undefined, NotSupExpect]),
    ok = em:replay(M),
    S = #state{socket=undefined},

    ?assertEqual(S, process_cmd(S, <<"c">>)),
    ?assertEqual(S, process_cmd(S, <<"check">>)),
    ?assertEqual(S, process_cmd(S, <<"m">>)),
    ?assertEqual(S, process_cmd(S, <<"multi">>)),
    ?assertEqual(S, process_cmd(S, <<"s">>)),
    ?assertEqual(S, process_cmd(S, <<"set">>)),
    ?assertEqual(S, process_cmd(S, <<"b">>)),
    ?assertEqual(S, process_cmd(S, <<"bulk">>)),

    ?assertEqual(S, process_cmd(S, <<"info">>)),
    ?assertEqual(S, process_cmd(S, <<"drop">>)),
    ?assertEqual(S, process_cmd(S, <<"close">>)),
    ?assertEqual(S, process_cmd(S, <<"clear">>)),
    ?assertEqual(S, process_cmd(S, <<"create">>)),

    ?assertEqual(S, process_cmd(S, <<"tubez">>)),
    em:verify(M).

flush_all_test() ->
    cmd_test(flush, [undefined], {ok, done},
             <<"flush">>, [?DONE]).

flush_all_timeout_test() ->
    cmd_test(flush, [undefined], {error, timeout},
             <<"flush">>, [?INTERNAL_ERR]).

flush_filt_test() ->
    cmd_test(flush, [<<"foo">>], {ok, done},
             <<"flush foo">>, [?DONE]).

flush_filt_missing_test() ->
    cmd_test(flush, [<<"foo">>], {error, no_filter},
             <<"flush foo">>, [?FILT_NOT_EXIST]).

flush_filt_timeout_test() ->
    cmd_test(flush, [<<"foo">>], {error, timeout},
             <<"flush foo">>, [?INTERNAL_ERR]).

list_test() ->
    List = [{<<"tubez">>, [{probability, 0.001},
                           {bytes, 100},
                           {capacity, 1000},
                          {size, 10}]}],
    Out = [?START,
           [[[<<"tubez">>, ?SPACE,
              ["0",".",["0","0"],"1000"], ?SPACE,
              "100", ?SPACE,
              "1000", ?SPACE,
              "10"], <<"\n">>]],
    ?END],
    cmd_test(list, [false], {ok, List},
             <<"list">>, Out).


list_abs_test() ->
    List = [{<<"tubez">>, [{probability, 0.001},
                           {bytes, 100},
                           {capacity, 1000},
                          {size, 10}]}],
    Out = [?START,
           [[[<<"tubez">>, ?SPACE,
              ["0",".",["0","0"],"1000"], ?SPACE,
              "100", ?SPACE,
              "1000", ?SPACE,
              "10"], <<"\n">>]],
    ?END],
    cmd_test(list, [true], {ok, List},
             <<"list +absolute">>, Out).

list_unexp_test() ->
    cmd_test(undefined, [], [],
             <<"list +foo">>, [?CLIENT_ERR, ?UNEXPECTED_ARGS, ?NEWLINE]).

list_error_test() ->
    cmd_test(list, [false], {error, timeout},
             <<"list">>, [?INTERNAL_ERR]).

create_no_filter_test() ->
    cmd_test(undefined, [], [],
             <<"create ">>, [?CLIENT_ERR, ?FILT_NEEDED, ?NEWLINE]).

create_bad_filter_test() ->
    cmd_test(undefined, [], [],
             <<"create \t">>, [?CLIENT_ERR, ?BAD_FILT_NAME, ?NEWLINE]).

create_bad_args_test() ->
    cmd_test(undefined, [], [],
             <<"create foo tubez=1">>, [?CLIENT_ERR, ?BAD_ARGS, ?NEWLINE]).

create_exists_test() ->
    cmd_test(create, [<<"foo">>, [{capacity, 10}]], {ok, exists},
             <<"create foo capacity=10">>, [?EXISTS]).

create_done_test() ->
    cmd_test(create, [<<"foo">>, [{capacity, 10}]], {ok, done},
             <<"create foo capacity=10">>, [?DONE]).

create_error_test() ->
    cmd_test(create, [<<"foo">>, [{capacity, 10}]], {error, timeout},
             <<"create foo capacity=10">>, [?INTERNAL_ERR]).

create_client_error_test() ->
    cmd_test(create, [<<"foo">>, [{capacity, 10}]], {error, {client_error, <<"tubez">>}},
             <<"create foo capacity=10">>, [?CLIENT_ERR, <<"tubez">>, ?NEWLINE]).

clear_done_test() ->
    cmd_test(clear, [<<"foo">>], {ok, done},
             <<"clear foo">>, [?DONE]).

clear_error_test() ->
    cmd_test(clear, [<<"foo">>], {error, timeout},
             <<"clear foo">>, [?INTERNAL_ERR]).

clear_no_filt_test() ->
    cmd_test(clear, [<<"foo">>], {error, no_filter},
             <<"clear foo">>, [?FILT_NOT_EXIST]).

clear_not_proxied_test() ->
    cmd_test(clear, [<<"foo">>], {error, not_proxied},
             <<"clear foo">>, [?FILT_NOT_PROXIED]).

close_done_test() ->
    cmd_test(close, [<<"foo">>], {ok, done},
             <<"close foo">>, [?DONE]).

close_error_test() ->
    cmd_test(close, [<<"foo">>], {error, timeout},
             <<"close foo">>, [?INTERNAL_ERR]).

close_no_filt_test() ->
    cmd_test(close, [<<"foo">>], {error, no_filter},
             <<"close foo">>, [?FILT_NOT_EXIST]).

drop_done_test() ->
    cmd_test(drop, [<<"foo">>], {ok, done},
             <<"drop foo">>, [?DONE]).

drop_error_test() ->
    cmd_test(drop, [<<"foo">>], {error, timeout},
             <<"drop foo">>, [?INTERNAL_ERR]).

drop_no_filt_test() ->
    cmd_test(drop, [<<"foo">>], {error, no_filter},
             <<"drop foo">>, [?FILT_NOT_EXIST]).

valid_info_test() ->
    Props = [{probability, 0.001}, {capacity, 1000}],
    Expect = [?START,
              [[["probability", ?SPACE, ["0", ".", ["0", "0"], "1000"]], ?NEWLINE],
               [["capacity", ?SPACE, "1000"], ?NEWLINE]],
              ?END],
    cmd_test(info, [<<"foo">>, false], {ok, Props},
             <<"info foo">>, Expect).

absolute_valid_info_test() ->
    Props = [{probability, 0.001}, {capacity, 1000}],
    Expect = [?START,
              [[["probability", ?SPACE, ["0", ".", ["0", "0"], "1000"]], ?NEWLINE],
               [["capacity", ?SPACE, "1000"], ?NEWLINE]],
              ?END],
    cmd_test(info, [<<"foo">>, true], {ok, Props},
             <<"info foo +absolute">>, Expect).

error_info_test() ->
    cmd_test(info, [<<"foo">>, false], {error, timeout},
             <<"info foo">>, [?INTERNAL_ERR]).

noexist_info_test() ->
    cmd_test(info, [<<"foo">>, false], {error, no_filter},
             <<"info foo">>, [?FILT_NOT_EXIST]).

nofilt_info_test() ->
    cmd_test(undefined, [], [],
             <<"info ">>, [?CLIENT_ERR, ?FILT_NEEDED, ?NEWLINE]).

badfilt_info_test() ->
    cmd_test(undefined, [], [],
             <<"info \t">>, [?CLIENT_ERR, ?BAD_FILT_NAME, ?NEWLINE]).

badflag_info_test() ->
    cmd_test(undefined, [], [],
             <<"info foo +tubez">>, [?CLIENT_ERR, ?UNEXPECTED_ARGS, ?NEWLINE]).

alias_check_test() ->
    cmd_test(check, [<<"foo">>, <<"bar">>], {ok, true},
             <<"c foo bar">>, [?YES_RESP]).

check_test() ->
    cmd_test(check, [<<"foo">>, <<"bar">>], {ok, true},
             <<"check foo bar">>, [?YES_RESP]).

alias_set_test() ->
    cmd_test(set, [<<"foo">>, <<"bar">>], {ok, true},
             <<"s foo bar">>, [?YES_RESP]).

set_test() ->
    cmd_test(set, [<<"foo">>, <<"bar">>], {ok, true},
             <<"set foo bar">>, [?YES_RESP]).

alias_multi_test() ->
    cmd_test(multi, [<<"foo">>, [<<"bar">>, <<"baz">>]], {ok, [false, true]},
             <<"m foo bar baz">>, [?NO_SPACE, ?YES_RESP]).

multi_test() ->
    cmd_test(multi, [<<"foo">>, [<<"bar">>, <<"baz">>]], {ok, [false, true]},
             <<"multi foo bar baz">>, [?NO_SPACE, ?YES_RESP]).

alias_bulk_test() ->
    cmd_test(bulk, [<<"foo">>, [<<"bar">>, <<"baz">>]], {ok, [false, true]},
             <<"b foo bar baz">>, [?NO_SPACE, ?YES_RESP]).

bulk_test() ->
    cmd_test(bulk, [<<"foo">>, [<<"bar">>, <<"baz">>]], {ok, [false, true]},
             <<"bulk foo bar baz">>, [?NO_SPACE, ?YES_RESP]).

cmd_test(Cmd, Input, Output, Buffer, Write) ->
    M = em:new(),
    case Cmd of
        undefined -> ok;
        _ ->
            em:strict(M, bloomd_ring, Cmd, Input,
                            {return, Output})
    end,
    em:strict(M, gen_tcp, send, [undefined, Write]),
    ok = em:replay(M),
    S = #state{socket=undefined},
    ?assertEqual(S, process_cmd(S, Buffer)),
    em:verify(M).

empty_process_buffer_test() ->
    Buf = <<>>,
    S = #state{socket=undefined, buffer=Buf},
    ?assertEqual(S, process_buffer(S, <<>>)).

blankline_process_buffer_test() ->
    Buf = <<"\n">>,
    Blank = <<>>,
    S = #state{socket=undefined, buffer=Blank},
    ?assertEqual(S, process_buffer(S, Buf)).

line_process_buffer_test() ->
    M = em:new(),
    em:strict(M, gen_tcp, send, [undefined, [?CLIENT_ERR, ?CMD_NOT_SUP, ?NEWLINE]]),
    ok = em:replay(M),

    Buf = <<"tubez\n">>,
    Blank = <<>>,
    S = #state{socket=undefined, buffer=Blank},
    ?assertEqual(S, process_buffer(S, Buf)),
    em:verify(M).

-endif.
