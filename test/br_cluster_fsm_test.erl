-module(br_cluster_fsm_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

new_fsm(Op, Args) ->
    ReqId = erlang:make_ref(),
    From = self(),
    {ok, Pid} = br_cluster_fsm:start_link(ReqId, From, Op, Args),
    {ReqId, Pid}.

kill_fsm(Pid) ->
    unlink(Pid),
    exit(Pid, shutdown).

ignore_msg_test() ->
    % Mock out the ring
    Ring = riak_core_ring:fresh(64, tubez),
    Pref = [{foo, bar}],
    M = em:new(),
    em:strict(M, riak_core_ring_manager, get_my_ring, [], {return, {ok, Ring}}),
    em:strict(M, riak_core_ring, all_owners, [Ring], {return, Pref}),
    em:strict(M, riak_core_vnode_master, command,
              [Pref, list_filters, fun({fsm, undefined, _}) -> true end, br_vnode_master]),
    ok = em:replay(M),

    {_, Pid} = new_fsm(list, undefined),

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

timout_test() ->
    % Mock out the ring
    Ring = riak_core_ring:fresh(64, tubez),
    Pref = [{foo, bar}],
    M = em:new(),
    em:strict(M, riak_core_ring_manager, get_my_ring, [], {return, {ok, Ring}}),
    em:strict(M, riak_core_ring, all_owners, [Ring], {return, Pref}),
    em:strict(M, riak_core_vnode_master, command,
              [Pref, {info_filter, <<"tubez">>}, fun({fsm, undefined, _}) -> true end, br_vnode_master]),
    ok = em:replay(M),

    {ReqId, Pid} = new_fsm(info, <<"tubez">>),
    timer:sleep(20),

    % Send an artificial timeout
    Pid ! timeout,

    % Wait to get the error
    receive
        Msg -> ?assertEqual({ReqId, {error, timeout}}, Msg)
    after 200 ->
        ?assertEqual(false, true)
    end,
    kill_fsm(Pid),
    em:verify(M).

all_respond_test() ->
    % Mock out the ring
    Ring = riak_core_ring:fresh(64, tubez),
    Pref = [{foo, bar}, {tubez, baz}],
    M = em:new(),
    em:strict(M, riak_core_ring_manager, get_my_ring, [], {return, {ok, Ring}}),
    em:strict(M, riak_core_ring, all_owners, [Ring], {return, Pref}),
    em:strict(M, riak_core_vnode_master, command,
              [Pref, {flush_filter, <<"tubez">>}, fun({fsm, undefined, _}) -> true end, br_vnode_master]),
    ok = em:replay(M),

    {ReqId, Pid} = new_fsm(flush, <<"tubez">>),
    timer:sleep(50),

    % Send results
    gen_fsm:send_event(Pid, done),
    gen_fsm:send_event(Pid, done),

    % Wait to get the error
    receive
        Msg -> ?assertEqual({ReqId, {ok, [done, done]}}, Msg)
    after 200 ->
        ?assertEqual(false, true)
    end,
    kill_fsm(Pid),
    em:verify(M).

