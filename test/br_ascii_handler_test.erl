-module(br_ascii_handler_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

main_test_() ->
    {foreach,
     spawn,
     fun setup/0,
     fun cleanup/1,
     [
      fun start_socket/1,
      fun call_socket/1,
      fun data_receive/1,
      fun tcp_error/1,
      fun tcp_closed/1
     ]}.

setup() ->
    case br_ascii_handler:start_link(undefined) of
        {ok, P} -> P;
        {error, {already_started, P}} -> P
    end.

cleanup(Pid) ->
    unlink(Pid),
    exit(Pid, shutdown),
    timer:sleep(20).

start_socket(Pid) ->
    ?_test(
        begin
            M = em:new(),
            em:strict(M, inet, setopts, [undefined, [{active, once}, {nodelay, true}]]),
            ok = em:replay(M),

            gen_server:cast(Pid, start),
            timer:sleep(1),
            {status, _, _, _} = sys:get_status(Pid),
            em:verify(M)
        end
    ).

call_socket(Pid) ->
    ?_test(
        begin
            ?assertEqual(ok, gen_server:call(Pid, tubez))
        end
    ).

data_receive(Pid) ->
    ?_test(
        begin
            M = em:new(),
            em:strict(M, gen_tcp, send, [undefined, [<<"Client Error: ">>, <<"Command not supported">>, <<"\n">>]]),
            em:strict(M, inet, setopts, [undefined, [{active, once}]]),
            ok = em:replay(M),

            Pid ! {tcp, undefined, [<<"tubez\n">>]},

            timer:sleep(1),
            {status, _, _, _} = sys:get_status(Pid),
            em:verify(M)
        end
    ).

tcp_error(Pid) ->
    ?_test(
        begin
            process_flag(trap_exit, true),
            link(Pid),
            Pid ! {tcp_error, undefined, epipe},
            receive
                {'EXIT', Pid, normal} -> ok
            after 500 ->
                ?assertEqual(true, false)
            end
        end
    ).

tcp_closed(Pid) ->
    ?_test(
        begin
            process_flag(trap_exit, true),
            link(Pid),
            Pid ! {tcp_closed, undefined},
            receive
                {'EXIT', Pid, normal} -> ok
            after 500 ->
                ?assertEqual(true, false)
            end
        end
    ).

