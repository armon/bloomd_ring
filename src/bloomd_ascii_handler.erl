-module(bloomd_ascii_handler).
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
-define(VALID_FILT_RE, "^[a-zA-Z0-9._-]{1,200}$").


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
    {stop, normal, State}.


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

process_cmd(State=#state{socket=Sock}, <<"c ", _Rest/binary>>) ->
    State;
process_cmd(State=#state{socket=Sock}, <<"check ", _Rest/binary>>) ->
    State;

process_cmd(State=#state{socket=Sock}, <<"m ", _Rest/binary>>) ->
    State;
process_cmd(State=#state{socket=Sock}, <<"multi ", _Rest/binary>>) ->
    State;


process_cmd(State=#state{socket=Sock}, <<"s ", _Rest/binary>>) ->
    State;
process_cmd(State=#state{socket=Sock}, <<"set ", _Rest/binary>>) ->
    State;

process_cmd(State=#state{socket=Sock}, <<"b ", _Rest/binary>>) ->
    State;
process_cmd(State=#state{socket=Sock}, <<"bulk ", _Rest/binary>>) ->
    State;

process_cmd(State=#state{socket=Sock}, <<"info ", _Rest/binary>>) ->
    State;

process_cmd(State=#state{socket=Sock}, <<"drop ", _Rest/binary>>) ->
    State;

process_cmd(State=#state{socket=Sock}, <<"close ", _Rest/binary>>) ->
    State;

process_cmd(State=#state{socket=Sock}, <<"clear ", _Rest/binary>>) ->
    State;

process_cmd(State=#state{socket=Sock}, <<"create ", _Rest/binary>>) ->
    State;

process_cmd(State=#state{socket=Sock}, <<"list">>) ->
    State;

% Handle the filter vs no-filter case
process_cmd(State=#state{socket=Sock}, <<"flush ", _Rest/binary>>) ->
    State;
process_cmd(State=#state{socket=Sock}, <<"flush">>) ->
    State;


% Catch all for an undefined command
process_cmd(State=#state{socket=Sock}, _) ->
    gen_tcp:send(Sock, [?CLIENT_ERR, ?CMD_NOT_SUP, ?NEWLINE]), State.


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
    SubPart = abs(trunc(Val * 10000)) rem 10000,

    % Convert to iolist
    [integer_to_list(WholePart), ".", integer_to_list(SubPart)].



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
    ?assertEqual(["-123", ".", "1234"], format_float(-123.123456)),
    ?assertEqual(["123", ".", "1234"], format_float(123.123456)).

-endif.
