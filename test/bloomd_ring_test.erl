-module(bloomd_ring_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

ping_test() ->
    M = em:new(),
    em:strict(M, riak_core_util, chash_key, [fun({<<"ping">>, _}) -> true end], {return, 0}),
    em:strict(M, riak_core_apl, get_primary_apl, [0, 1, bloomd], {return, [{{0, tubez}, primary}]}),
    em:strict(M, riak_core_vnode_master, sync_spawn_command, [{0, tubez}, ping, br_vnode_master],
              {return, {pong, 0}}),
    ok = em:replay(M),
    ?assertEqual({pong, 0}, bloomd_ring:ping()),
    em:verify(M).

create_basic_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [create, {<<"foo">>, []}], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate a response
    S = self(),
    spawn(fun() -> timer:sleep(20), S ! {req, {ok, [done, done, done, exists]}} end),

    ?assertEqual({ok, done}, bloomd_ring:create(<<"foo">>, [])),
    em:verify(M).

create_options_test() ->
    M = em:new(),
    em:strict(M, br_util, num_partitions, [], {return, 64}),
    em:strict(M, br_util, ceiling, [234375.0], {return, 234375}),
    em:strict(M, br_cluster_fsm, start_op, [create, {<<"foo">>, [{capacity, 234375}]}],
              {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate a response
    S = self(),
    spawn(fun() -> timer:sleep(20), S ! {req, {ok, [done, done, done, exists]}} end),

    ?assertEqual({ok, done}, bloomd_ring:create(<<"foo">>, [{capacity, 15000000}, {in_memory, 1}])),
    em:verify(M).

create_timeout_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [create, {<<"foo">>, []}], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate a response
    S = self(),
    spawn(fun() -> timer:sleep(20), S ! {req, {error, timeout}} end),

    ?assertEqual({error, timeout}, bloomd_ring:create(<<"foo">>, [])),
    em:verify(M).

create_exists_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [create, {<<"foo">>, []}], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate a response
    S = self(),
    spawn(fun() -> timer:sleep(20), S ! {req, {ok, [exists, exists, exists, exists]}} end),

    ?assertEqual({ok, exists}, bloomd_ring:create(<<"foo">>, [])),
    em:verify(M).

create_badoptions_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [create, {<<"foo">>, []}], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate a response
    S = self(),
    spawn(fun() -> timer:sleep(20), S ! {req, {ok, [
                            {error, {client_error, <<"Bad Args">>}},
                             {error, {client_error, <<"Bad Args">>}}
                        ]}} end),

    ?assertEqual({error, {client_error, <<"Bad Args">>}}, bloomd_ring:create(<<"foo">>, [])),
    em:verify(M).

list_absolute_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [list, undefined], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    Results = [{<<"test">>, [{1, [{capacity, 1000}, {probability, 0.001}]},
                             {2, [{capacity, 1000}, {probability, 0.001}]}]}],
    S = self(),
    spawn(fun() -> S ! {req, {ok, [{ok, Results}]}} end),

    % Check results
    Info = [{<<"test">>, [{capacity, 2000}, {probability, 0.001}]}],
    ?assertEqual({ok, Info}, bloomd_ring:list(true)),
    em:verify(M).

list_coverage_test() ->
    M = em:new(),
    em:strict(M, br_coverage_fsm, start_op, [list, undefined], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {ok, [{error, command_failed}]}} end),

    % Check results
    ?assertEqual({error, internal_error}, bloomd_ring:list(false)),
    em:verify(M).

list_timeout_test() ->
    M = em:new(),
    em:strict(M, br_coverage_fsm, start_op, [list, undefined], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {error, timeout}} end),

    % Check results
    ?assertEqual({error, timeout}, bloomd_ring:list(false)),
    em:verify(M).

drop_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [drop, <<"foo">>], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {ok, [done, done, {error, no_filter}]}} end),

    % Check results
    ?assertEqual({ok, done}, bloomd_ring:drop(<<"foo">>)),
    em:verify(M).

drop_timeout_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [drop, <<"foo">>], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {error, timeout}} end),

    % Check results
    ?assertEqual({error, timeout}, bloomd_ring:drop(<<"foo">>)),
    em:verify(M).

drop_nofilt_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [drop, <<"foo">>], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {ok, [{error, no_filter}, {error, no_filter}, {error, no_filter}]}} end),

    % Check results
    ?assertEqual({error, no_filter}, bloomd_ring:drop(<<"foo">>)),
    em:verify(M).

close_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [close, <<"foo">>], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {ok, [done, done, {error, no_filter}]}} end),

    % Check results
    ?assertEqual({ok, done}, bloomd_ring:close(<<"foo">>)),
    em:verify(M).

close_timeout_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [close, <<"foo">>], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {error, timeout}} end),

    % Check results
    ?assertEqual({error, timeout}, bloomd_ring:close(<<"foo">>)),
    em:verify(M).

close_nofilt_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [close, <<"foo">>], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {ok, [{error, no_filter}, {error, no_filter}, {error, no_filter}]}} end),

    % Check results
    ?assertEqual({error, no_filter}, bloomd_ring:close(<<"foo">>)),
    em:verify(M).

clear_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [clear, <<"foo">>], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {ok, [done, done, {error, no_filter}]}} end),

    % Check results
    ?assertEqual({ok, done}, bloomd_ring:clear(<<"foo">>)),
    em:verify(M).

clear_timeout_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [clear, <<"foo">>], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {error, timeout}} end),

    % Check results
    ?assertEqual({error, timeout}, bloomd_ring:clear(<<"foo">>)),
    em:verify(M).

clear_nofilt_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [clear, <<"foo">>], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {ok, [{error, no_filter}, {error, no_filter}, {error, no_filter}]}} end),

    % Check results
    ?assertEqual({error, no_filter}, bloomd_ring:clear(<<"foo">>)),
    em:verify(M).

clear_notproxied_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [clear, <<"foo">>], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {ok, [{error, no_filter}, {error, no_filter}, {error, not_proxied}]}} end),

    % Check results
    ?assertEqual({error, not_proxied}, bloomd_ring:clear(<<"foo">>)),
    em:verify(M).


check_test() ->
    M = em:new(),
    em:strict(M, br_quorum_fsm, start_op, [check, {<<"foo">>, <<"bar">>}], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {ok, true}} end),

    ?assertEqual({ok, true}, bloomd_ring:check(<<"foo">>, <<"bar">>)),
    em:verify(M).


check_timeout_test() ->
    M = em:new(),
    em:strict(M, br_quorum_fsm, start_op, [check, {<<"foo">>, <<"bar">>}], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {error, timeout}} end),

    ?assertEqual({error, timeout}, bloomd_ring:check(<<"foo">>, <<"bar">>)),
    em:verify(M).

check_nofilt_test() ->
    M = em:new(),
    em:strict(M, br_quorum_fsm, start_op, [check, {<<"foo">>, <<"bar">>}], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {error, no_filter}} end),

    ?assertEqual({error, no_filter}, bloomd_ring:check(<<"foo">>, <<"bar">>)),
    em:verify(M).

set_test() ->
    M = em:new(),
    em:strict(M, br_quorum_fsm, start_op, [set, {<<"foo">>, <<"bar">>}], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {ok, true}} end),

    ?assertEqual({ok, true}, bloomd_ring:set(<<"foo">>, <<"bar">>)),
    em:verify(M).


set_timeout_test() ->
    M = em:new(),
    em:strict(M, br_quorum_fsm, start_op, [set, {<<"foo">>, <<"bar">>}], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {error, timeout}} end),

    ?assertEqual({error, timeout}, bloomd_ring:set(<<"foo">>, <<"bar">>)),
    em:verify(M).

set_nofilt_test() ->
    M = em:new(),
    em:strict(M, br_quorum_fsm, start_op, [set, {<<"foo">>, <<"bar">>}], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {error, no_filter}} end),

    ?assertEqual({error, no_filter}, bloomd_ring:set(<<"foo">>, <<"bar">>)),
    em:verify(M).

multi_test() ->
    M = em:new(),
    em:strict(M, rpc, pmap, [{bloomd_ring, pmap_cmd}, [check, [<<"foo">>]], [<<"bop">>, <<"baz">>, <<"bar">>]],
              {return, [{ok, true}, {ok, true}, {ok, false}]}),
    ok = em:replay(M),

    ?assertEqual({ok, [true, true, false]}, bloomd_ring:multi(<<"foo">>, [<<"bop">>, <<"baz">>, <<"bar">>])),
    em:verify(M).

multi_nofilt_test() ->
    M = em:new(),
    em:strict(M, rpc, pmap, [{bloomd_ring, pmap_cmd}, [check, [<<"foo">>]], [<<"bop">>, <<"baz">>, <<"bar">>]],
              {return, [{ok, true}, {ok, true}, {error, no_filter}]}),
    ok = em:replay(M),

    ?assertEqual({error, no_filter}, bloomd_ring:multi(<<"foo">>, [<<"bop">>, <<"baz">>, <<"bar">>])),
    em:verify(M).

bulk_test() ->
    M = em:new(),
    em:strict(M, rpc, pmap, [{bloomd_ring, pmap_cmd}, [set, [<<"foo">>]], [<<"bop">>, <<"baz">>, <<"bar">>]],
              {return, [{ok, true}, {ok, true}, {ok, false}]}),
    ok = em:replay(M),

    ?assertEqual({ok, [true, true, false]}, bloomd_ring:bulk(<<"foo">>, [<<"bop">>, <<"baz">>, <<"bar">>])),
    em:verify(M).

bulk_nofilt_test() ->
    M = em:new(),
    em:strict(M, rpc, pmap, [{bloomd_ring, pmap_cmd}, [set, [<<"foo">>]], [<<"bop">>, <<"baz">>, <<"bar">>]],
              {return, [{ok, true}, {ok, true}, {error, no_filter}]}),
    ok = em:replay(M),

    ?assertEqual({error, no_filter}, bloomd_ring:bulk(<<"foo">>, [<<"bop">>, <<"baz">>, <<"bar">>])),
    em:verify(M).


info_absolute_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [info, <<"foo">>], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    Results = [{1, [{capacity, 1000}, {probability, 0.001}]},
                             {2, [{capacity, 1000}, {probability, 0.001}]}],
    S = self(),
    spawn(fun() -> S ! {req, {ok, [{ok, Results}]}} end),

    % Check results
    Info = [{capacity, 2000}, {probability, 0.001}],
    ?assertEqual({ok, Info}, bloomd_ring:info(<<"foo">>, true)),
    em:verify(M).

info_coverage_test() ->
    M = em:new(),
    em:strict(M, br_coverage_fsm, start_op, [info, <<"foo">>], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {ok, [{error, no_filter}, {error, no_filter}]}} end),

    % Check results
    ?assertEqual({error, no_filter}, bloomd_ring:info(<<"foo">>, false)),
    em:verify(M).

info_timeout_test() ->
    M = em:new(),
    em:strict(M, br_coverage_fsm, start_op, [info, <<"foo">>], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {error, timeout}} end),

    % Check results
    ?assertEqual({error, timeout}, bloomd_ring:info(<<"foo">>, false)),
    em:verify(M).

flush_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [flush, <<"foo">>], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {ok, [done, done, {error, no_filter}]}} end),

    % Check results
    ?assertEqual({ok, done}, bloomd_ring:flush(<<"foo">>)),
    em:verify(M).

flush_timeout_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [flush, <<"foo">>], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {error, timeout}} end),

    % Check results
    ?assertEqual({error, timeout}, bloomd_ring:flush(<<"foo">>)),
    em:verify(M).

flush_nofilt_test() ->
    M = em:new(),
    em:strict(M, br_cluster_fsm, start_op, [flush, <<"foo">>], {return, {ok, req}}),
    ok = em:replay(M),

    % Simulate response
    S = self(),
    spawn(fun() -> S ! {req, {ok, [{error, no_filter}, {error, no_filter}, {error, no_filter}]}} end),

    % Check results
    ?assertEqual({error, no_filter}, bloomd_ring:flush(<<"foo">>)),
    em:verify(M).


