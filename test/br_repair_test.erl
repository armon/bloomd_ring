-module(br_repair_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

main_test_() ->
    {foreach,
     spawn,
     fun setup/0,
     fun cleanup/1,
     [
      fun invalid_down/1,
      fun invalid_cmds/1,
      fun should_repair_true/1,
      fun should_repair_false/1,
      fun should_repair_exit/1
     ]}.

setup() ->
    case br_repair:start_link() of
        {ok, P} -> P;
        {error, {already_started, P}} -> P
    end.

cleanup(Pid) ->
    unlink(Pid),
    exit(Pid, shutdown),
    timer:sleep(20).

invalid_down(Pid) ->
    ?_test(
        begin
            Pid ! {'DOWN', undefined, process, badpid, normal},
            timer:sleep(1),
            {status, _, _, _} = sys:get_status(Pid)
        end
    ).

invalid_cmds(Pid) ->
    ?_test(
        begin
            Pid ! useless,
            gen_server:cast(Pid, useless),
            try gen_server:call(Pid, useless, 0)
            catch
                exit:_ -> ok
            end,
            {status, _, _, _} = sys:get_status(Pid)
        end
    ).

should_repair_true(Pid) ->
    ?_test(
        begin
            process_flag(trap_exit, true),
            P = spawn_link(fun() ->
                true = gen_server:call(Pid, {should_repair, <<"tubez">>, self()})
            end),
            receive
                {'EXIT', P, normal} -> ok
            after 200 ->
                ?assertEqual(true, false)
            end
        end
    ).

should_repair_false(Pid) ->
    ?_test(
        begin
            process_flag(trap_exit, true),
            P = spawn_link(fun() ->
                true = gen_server:call(Pid, {should_repair, <<"tubez">>, self()}),
                false = gen_server:call(Pid, {should_repair, <<"tubez">>, self()})
            end),
            receive
                {'EXIT', P, normal} -> ok
            after 200 ->
                ?assertEqual(true, false)
            end
        end
    ).

should_repair_exit(Pid) ->
    ?_test(
        begin
            process_flag(trap_exit, true),

            % Should return true on first go
            P = spawn_link(fun() ->
                true = gen_server:call(Pid, {should_repair, <<"tubez">>, self()}),
                exit(bad)
            end),
            receive
                {'EXIT', P, bad} -> ok
            after 200 ->
                ?assertEqual(true, false)
            end,

            % It should return true again
            P1 = spawn_link(fun() ->
                true = gen_server:call(Pid, {should_repair, <<"tubez">>, self()})
            end),
            receive
                {'EXIT', P1, normal} -> ok
            after 200 ->
                ?assertEqual(true, false)
            end
        end
    ).

