-module(br_quorum_fsm_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

new_fsm(Op, Args) ->
    ReqId = erlang:make_ref(),
    {ok, Pid} = br_quorum_fsm:start_link([]),
    ok = gen_fsm:send_event(Pid, {init, ReqId, self(), Op, Args}),
    {ReqId, Pid}.

kill_fsm(Pid) ->
    unlink(Pid),
    exit(Pid, shutdown).

ignore_msg_test() ->
    % Mock out some calls
    Pref = [{0, tubez}, {1, noobz}, {2, foo}],
    M = em:new(),
    em:strict(M, riak_core_ring_manager, get_my_ring, [], {return, {ok, ring}}),
    em:strict(M, riak_core_ring, num_partitions, [ring], {return, 64}),
    em:strict(M, br_util, keyslice, [<<"bar">>, 64], {return, 12}),
    em:strict(M, br_util, hash_slice, [<<"foo">>, 12], {return, 42}),
    em:strict(M, riak_core_ring, preflist, [42, ring], {return, Pref}),
    em:strict(M, riak_core_vnode_master, command,
              [Pref, {check_filter, <<"foo">>, 12, <<"bar">>},
               fun({fsm, undefined, _}) -> true end, br_vnode_master]),
    ok = em:replay(M),

    {_, Pid} = new_fsm(check, {<<"foo">>, <<"bar">>}),

    % Send various shit
    Pid ! tubez,
    gen_fsm:send_all_state_event(Pid, tubez),
    try gen_fsm:sync_send_all_state_event(Pid, tubez, 1)
    catch
        exit:_ -> ok
    end,
    timer:sleep(50),
    kill_fsm(Pid),
    em:verify(M).


timeout_test() ->
    % Mock out some calls
    Pref = [{0, tubez}, {1, noobz}, {2, foo}],
    M = em:new(),
    em:strict(M, riak_core_ring_manager, get_my_ring, [], {return, {ok, ring}}),
    em:strict(M, riak_core_ring, num_partitions, [ring], {return, 64}),
    em:strict(M, br_util, keyslice, [<<"bar">>, 64], {return, 12}),
    em:strict(M, br_util, hash_slice, [<<"foo">>, 12], {return, 42}),
    em:strict(M, riak_core_ring, preflist, [42, ring], {return, Pref}),
    em:strict(M, riak_core_vnode_master, command,
              [Pref, {set_filter, <<"foo">>, 12, <<"bar">>},
               fun({fsm, undefined, _}) -> true end, br_vnode_master]),
    ok = em:replay(M),

    {ReqId, Pid} = new_fsm(set, {<<"foo">>, <<"bar">>}),

    % Send artificial timeout
    timer:sleep(20),
    gen_fsm:send_event(Pid, timeout),

    % Wait for the error
    receive
        Msg -> ?assertEqual({ReqId, {error, timeout}}, Msg)
    after 200 ->
        ?assertEqual(false, true)
    end,
    kill_fsm(Pid),
    em:verify(M).


fast_consensus_test() ->
    % Mock out some calls
    Pref = [{0, tubez}, {1, noobz}, {2, foo}],
    M = em:new(),
    em:strict(M, riak_core_ring_manager, get_my_ring, [], {return, {ok, ring}}),
    em:strict(M, riak_core_ring, num_partitions, [ring], {return, 64}),
    em:strict(M, br_util, keyslice, [<<"bar">>, 64], {return, 12}),
    em:strict(M, br_util, hash_slice, [<<"foo">>, 12], {return, 42}),
    em:strict(M, riak_core_ring, preflist, [42, ring], {return, Pref}),
    em:strict(M, riak_core_vnode_master, command,
              [Pref, {set_filter, <<"foo">>, 12, <<"bar">>},
               fun({fsm, undefined, _}) -> true end, br_vnode_master]),
    ok = em:replay(M),

    {ReqId, Pid} = new_fsm(set, {<<"foo">>, <<"bar">>}),

    % Send artificial timeout
    timer:sleep(20),
    gen_fsm:send_event(Pid, {ok, true}),
    gen_fsm:send_event(Pid, {ok, true}),
    gen_fsm:send_event(Pid, {ok, true}),

    % Wait for the error
    receive
        Msg -> ?assertEqual({ReqId, {ok, true}}, Msg)
    after 200 ->
        ?assertEqual(false, true)
    end,
    kill_fsm(Pid),
    em:verify(M).


no_consensus_test() ->
    % Mock out some calls
    Pref = [{0, tubez}, {1, noobz}, {2, foo}],
    M = em:new(),
    em:strict(M, riak_core_ring_manager, get_my_ring, [], {return, {ok, ring}}),
    em:strict(M, riak_core_ring, num_partitions, [ring], {return, 64}),
    em:strict(M, br_util, keyslice, [<<"bar">>, 64], {return, 12}),
    em:strict(M, br_util, hash_slice, [<<"foo">>, 12], {return, 42}),
    em:strict(M, riak_core_ring, preflist, [42, ring], {return, Pref}),
    em:strict(M, riak_core_vnode_master, command,
              [Pref, {set_filter, <<"foo">>, 12, <<"bar">>},
               fun({fsm, undefined, _}) -> true end, br_vnode_master]),
        ok = em:replay(M),

    {ReqId, Pid} = new_fsm(set, {<<"foo">>, <<"bar">>}),

    % Send artificial timeout
    timer:sleep(20),
    gen_fsm:send_event(Pid, {ok, true}),
    gen_fsm:send_event(Pid, {ok, false}),
    gen_fsm:send_event(Pid, {error, command_failed}),

    % Wait for the response
    receive
        Msg -> ?assertEqual({ReqId, {ok, true}}, Msg)
    after 200 ->
        ?assertEqual(false, true)
    end,
    kill_fsm(Pid),
    em:verify(M).

key_repair_test() ->
    % Mock out some calls
    Pref = [{0, tubez}, {1, noobz}, {2, foo}],
    M = em:new(),
    em:strict(M, riak_core_ring_manager, get_my_ring, [], {return, {ok, ring}}),
    em:strict(M, riak_core_ring, num_partitions, [ring], {return, 64}),
    em:strict(M, br_util, keyslice, [<<"bar">>, 64], {return, 12}),
    em:strict(M, br_util, hash_slice, [<<"foo">>, 12], {return, 42}),
    em:strict(M, riak_core_ring, preflist, [42, ring], {return, Pref}),
    em:strict(M, riak_core_vnode_master, command,
              [Pref, {check_filter, <<"foo">>, 12, <<"bar">>},
               fun({fsm, undefined, _}) -> true end, br_vnode_master]),
    em:strict(M, bloomd_ring, set, [<<"foo">>, <<"bar">>]),
    em:strict(M, poolboy, checkin, [quorum_pool, em:zelf()]),
    ok = em:replay(M),

    {ReqId, Pid} = new_fsm(check, {<<"foo">>, <<"bar">>}),

    % Send artificial timeout
    timer:sleep(20),
    gen_fsm:send_event(Pid, {ok, true}),
    gen_fsm:send_event(Pid, {ok, false}),
    gen_fsm:send_event(Pid, {ok, true}),

    % Wait for the response
    receive
        Msg -> ?assertEqual({ReqId, {ok, true}}, Msg)
    after 200 ->
        ?assertEqual(false, true)
    end,
    timer:sleep(10),
    kill_fsm(Pid),
    em:verify(M).

filter_drop_repair_test() ->
    % Mock out some calls
    Pref = [{0, tubez}, {1, noobz}, {2, foo}],
    M = em:new(),
    em:strict(M, riak_core_ring_manager, get_my_ring, [], {return, {ok, ring}}),
    em:strict(M, riak_core_ring, num_partitions, [ring], {return, 64}),
    em:strict(M, br_util, keyslice, [<<"bar">>, 64], {return, 12}),
    em:strict(M, br_util, hash_slice, [<<"foo">>, 12], {return, 42}),
    em:strict(M, riak_core_ring, preflist, [42, ring], {return, Pref}),
    em:strict(M, riak_core_vnode_master, command,
              [Pref, {check_filter, <<"foo">>, 12, <<"bar">>},
               fun({fsm, undefined, _}) -> true end, br_vnode_master]),
    em:strict(M, br_repair, maybe_repair, [<<"foo">>]),
    em:strict(M, poolboy, checkin, [quorum_pool, em:zelf()]),
    ok = em:replay(M),

    {ReqId, Pid} = new_fsm(check, {<<"foo">>, <<"bar">>}),

    % Looks like drop should happen
    timer:sleep(20),
    gen_fsm:send_event(Pid, {ok, true}),
    gen_fsm:send_event(Pid, {error, no_filter}),
    gen_fsm:send_event(Pid, {error, no_filter}),

    % Wait for the response
    receive
        Msg -> ?assertEqual({ReqId, {error, no_filter}}, Msg)
    after 200 ->
        ?assertEqual(false, true)
    end,
    timer:sleep(20),
    kill_fsm(Pid),
    em:verify(M).


filter_create_repair_test() ->
    % Mock out some calls
    Pref = [{0, tubez}, {1, noobz}, {2, foo}],
    M = em:new(),
    em:strict(M, riak_core_ring_manager, get_my_ring, [], {return, {ok, ring}}),
    em:strict(M, riak_core_ring, num_partitions, [ring], {return, 64}),
    em:strict(M, br_util, keyslice, [<<"bar">>, 64], {return, 12}),
    em:strict(M, br_util, hash_slice, [<<"foo">>, 12], {return, 42}),
    em:strict(M, riak_core_ring, preflist, [42, ring], {return, Pref}),
    em:strict(M, riak_core_vnode_master, command,
              [Pref, {check_filter, <<"foo">>, 12, <<"bar">>},
               fun({fsm, undefined, _}) -> true end, br_vnode_master]),
    em:strict(M, br_repair, maybe_repair, [<<"foo">>]),
    em:strict(M, poolboy, checkin, [quorum_pool, em:zelf()]),
    ok = em:replay(M),

    {ReqId, Pid} = new_fsm(check, {<<"foo">>, <<"bar">>}),

    % Create shoudl happen
    timer:sleep(20),
    gen_fsm:send_event(Pid, {ok, true}),
    gen_fsm:send_event(Pid, {error, no_filter}),
    gen_fsm:send_event(Pid, {ok, false}),

    % Wait for the response
    receive
        Msg -> ?assertEqual({ReqId, {ok, true}}, Msg)
    after 200 ->
        ?assertEqual(false, true)
    end,
    timer:sleep(20), kill_fsm(Pid),
    em:verify(M).

filter_create_repair_agreed_test() ->
    % Mock out some calls
    Pref = [{0, tubez}, {1, noobz}, {2, foo}],
    M = em:new(),
    em:strict(M, riak_core_ring_manager, get_my_ring, [], {return, {ok, ring}}),
    em:strict(M, riak_core_ring, num_partitions, [ring], {return, 64}),
    em:strict(M, br_util, keyslice, [<<"bar">>, 64], {return, 12}),
    em:strict(M, br_util, hash_slice, [<<"foo">>, 12], {return, 42}),
    em:strict(M, riak_core_ring, preflist, [42, ring], {return, Pref}),
    em:strict(M, riak_core_vnode_master, command,
              [Pref, {check_filter, <<"foo">>, 12, <<"bar">>},
               fun({fsm, undefined, _}) -> true end, br_vnode_master]),
    em:strict(M, br_repair, maybe_repair, [<<"foo">>]),
    em:strict(M, poolboy, checkin, [quorum_pool, em:zelf()]),
    ok = em:replay(M),

    {ReqId, Pid} = new_fsm(check, {<<"foo">>, <<"bar">>}),

    % Create shoudl happen
    timer:sleep(20),
    gen_fsm:send_event(Pid, {ok, true}),
    gen_fsm:send_event(Pid, {error, no_filter}),
    gen_fsm:send_event(Pid, {ok, true}),

    % Wait for the response
    receive
        Msg -> ?assertEqual({ReqId, {ok, true}}, Msg)
    after 200 ->
        ?assertEqual(false, true)
    end,
    timer:sleep(20),
    kill_fsm(Pid),
    em:verify(M).

