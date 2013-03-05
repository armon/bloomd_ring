-module(br_vnode_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-define(MASTER, br_vnode_master).

new_vnode() -> new_vnode(0).
new_vnode(Partition) ->
    % Mock out the ring
    Ring = riak_core_ring:fresh(64, tubez),
    M = em:new(),
    em:strict(M, riak_core_ring_manager, get_my_ring, [], {return, {ok, Ring}}),
    ok = em:replay(M),

    % Mock out bloomd
    {ok, Sock} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(Sock),
    application:set_env(bloomd_ring, bloomd_local_host, "127.0.0.1"),
    application:set_env(bloomd_ring, bloomd_local_port, Port),

    % Start the vnode
    {ok, State} = br_vnode:init([Partition]),
    em:verify(M),

    % Get the client
    {ok, Client} = gen_tcp:accept(Sock, 1000),

    % Return all the relevant bits
    {Ring, Sock, Client, State}.


ping_test() ->
    {_, _, _, State} = new_vnode(0),
    {reply, Resp, State} = br_vnode:handle_command(ping, undefined, State),
    ?assertEqual({pong, 0}, Resp).

check_test() ->
    {_, _, _, State} = new_vnode(0),

    % Mock the bloomd:check call
    {state, _, _, _, Conn, _} = State,
    F = bloomd:filter(Conn, <<"foo">>),
    M = em:new(),
    em:strict(M, bloomd, filter, [Conn, ["0", <<":">>, <<"foo">>, <<":">>, "1"]], {return, F}),
    em:strict(M, bloomd, check, [F, <<"bar">>], {return, {ok, [true]}}),
    em:strict(M, riak_core_vnode, reply, [undefined, {ok, true}]),
    ok = em:replay(M),

    {noreply, State} = br_vnode:handle_command({check_filter, <<"foo">>, 1, <<"bar">>},
                                                   undefined, State),
    timer:sleep(150),
    em:verify(M).

set_test() ->
    {_, _, _, State} = new_vnode(0),

    % Mock the bloomd:check call
    {state, _, _, _, Conn, _} = State,
    F = bloomd:filter(Conn, <<"foo">>),
    M = em:new(),
    em:strict(M, bloomd, filter, [Conn, ["0", <<":">>, <<"foo">>, <<":">>, "42"]], {return, F}),
    em:strict(M, bloomd, set, [F, <<"zip">>], {return, {ok, [true]}}),
    em:strict(M, riak_core_vnode, reply, [undefined, {ok, true}]),
    ok = em:replay(M),

    {noreply, State} = br_vnode:handle_command({set_filter, <<"foo">>, 42, <<"zip">>},
                                                   undefined, State),
    timer:sleep(150),
    em:verify(M).

list_test() ->
    {_, _, Client, State} = new_vnode(0),

    % Simulate a response
    spawn_link(fun() ->
        {ok, Inp} = gen_tcp:recv(Client, 5),
        ?assertEqual(<<"list\n">>, Inp),
        Out = <<"START\n0:tubez:1 0.001 1000 500 200\n1:invalid:2 0.001 1000 500 200\nEND\n">>,
        gen_tcp:send(Client, Out)
    end),

    {reply, Resp, State} = br_vnode:handle_command(list_filters,
                                                   undefined, State),

    Info = [{<<"tubez">>, [{1, [{probability, 0.001}, {bytes, 1000}, {capacity, 500}, {size, 200}]}]}],
    ?assertEqual({ok, Info}, Resp).

drop_test() ->
    {_, _, Client, State} = new_vnode(0),

    % Simulate a response
    spawn_link(fun() ->
        {ok, Inp} = gen_tcp:recv(Client, 5),
        ?assertEqual(<<"list\n">>, Inp),
        Out = <<"START\n0:test:1 0.001 1000 500 200\nEND\n">>,
        gen_tcp:send(Client, Out),

        {ok, Inp2} = gen_tcp:recv(Client, 14),
        ?assertEqual(<<"drop 0:test:1\n">>, Inp2),
        gen_tcp:send(Client, <<"Done\n">>)
    end),

    {reply, Resp, State} = br_vnode:handle_command({drop_filter, <<"test">>},
                                                   undefined, State),
    ?assertEqual(done, Resp).

drop_nofilt_test() ->
    {_, _, Client, State} = new_vnode(0),

    % Simulate a response
    spawn_link(fun() ->
        {ok, Inp} = gen_tcp:recv(Client, 5),
        ?assertEqual(<<"list\n">>, Inp),
        Out = <<"START\nEND\n">>,
        gen_tcp:send(Client, Out)
    end),

    {reply, Resp, State} = br_vnode:handle_command({drop_filter, <<"test">>},
                                                   undefined, State),
    ?assertEqual({error, no_filter}, Resp).

close_test() ->
    {_, _, Client, State} = new_vnode(0),

    % Simulate a response
    spawn_link(fun() ->
        {ok, Inp} = gen_tcp:recv(Client, 5),
        ?assertEqual(<<"list\n">>, Inp),
        Out = <<"START\n0:test:1 0.001 1000 500 200\nEND\n">>,
        gen_tcp:send(Client, Out),

        {ok, Inp2} = gen_tcp:recv(Client, 15),
        ?assertEqual(<<"close 0:test:1\n">>, Inp2),
        gen_tcp:send(Client, <<"Done\n">>)
    end),

    {reply, Resp, State} = br_vnode:handle_command({close_filter, <<"test">>},
                                                   undefined, State),
    ?assertEqual(done, Resp).

close_nofilt_test() ->
    {_, _, Client, State} = new_vnode(0),

    % Simulate a response
    spawn_link(fun() ->
        {ok, Inp} = gen_tcp:recv(Client, 5),
        ?assertEqual(<<"list\n">>, Inp),
        Out = <<"START\nEND\n">>,
        gen_tcp:send(Client, Out)
    end),

    {reply, Resp, State} = br_vnode:handle_command({close_filter, <<"test">>},
                                                   undefined, State),
    ?assertEqual({error, no_filter}, Resp).

clear_test() ->
    {_, _, Client, State} = new_vnode(0),

    % Simulate a response
    spawn_link(fun() ->
        {ok, Inp} = gen_tcp:recv(Client, 5),
        ?assertEqual(<<"list\n">>, Inp),
        Out = <<"START\n0:test:1 0.001 1000 500 200\nEND\n">>,
        gen_tcp:send(Client, Out),

        {ok, Inp2} = gen_tcp:recv(Client, 15),
        ?assertEqual(<<"clear 0:test:1\n">>, Inp2),
        gen_tcp:send(Client, <<"Done\n">>)
    end),

    {reply, Resp, State} = br_vnode:handle_command({clear_filter, <<"test">>},
                                                   undefined, State),
    ?assertEqual(done, Resp).

clear_nofilt_test() ->
    {_, _, Client, State} = new_vnode(0),

    % Simulate a response
    spawn_link(fun() ->
        {ok, Inp} = gen_tcp:recv(Client, 5),
        ?assertEqual(<<"list\n">>, Inp),
        Out = <<"START\nEND\n">>,
        gen_tcp:send(Client, Out)
    end),

    {reply, Resp, State} = br_vnode:handle_command({clear_filter, <<"test">>},
                                                   undefined, State),
    ?assertEqual({error, no_filter}, Resp).

clear_notproxied_test() ->
    {_, _, Client, State} = new_vnode(0),

    % Simulate a response
    spawn_link(fun() ->
        {ok, Inp} = gen_tcp:recv(Client, 5),
        ?assertEqual(<<"list\n">>, Inp),
        Out = <<"START\n0:test:1 0.001 1000 500 200\n0:test:2 0.001 1000 500 200\nEND\n">>,
        gen_tcp:send(Client, Out),

        % Fail with not proxied
        {ok, Inp2} = gen_tcp:recv(Client, 15),
        ?assertEqual(<<"clear 0:test:2\n">>, Inp2),
        gen_tcp:send(Client, <<"Filter is not proxied. Close it first.\n">>),

        {ok, Inp5} = gen_tcp:recv(Client, 15),
        ?assertEqual(<<"clear 0:test:1\n">>, Inp5),
        gen_tcp:send(Client, <<"Done\n">>),

        % Should fault in 2 other slices
        {ok, Inp3} = gen_tcp:recv(Client, 16),
        ?assertEqual(<<"create 0:test:2\n">>, Inp3),
        gen_tcp:send(Client, <<"Done\n">>),

        {ok, Inp4} = gen_tcp:recv(Client, 16),
        ?assertEqual(<<"create 0:test:1\n">>, Inp4),
        gen_tcp:send(Client, <<"Done\n">>)
    end),

    {reply, Resp, State} = br_vnode:handle_command({clear_filter, <<"test">>},
                                                   undefined, State),
    ?assertEqual({error, not_proxied}, Resp).

flush_all_test() ->
    {_, _, Client, State} = new_vnode(0),

    % Simulate a response
    spawn_link(fun() ->
        {ok, Inp} = gen_tcp:recv(Client, 5),
        ?assertEqual(<<"list\n">>, Inp),
        Out = <<"START\n0:test:1 0.001 1000 500 200\n1:foo:2 0.001 1000 500 200\nEND\n">>,
        gen_tcp:send(Client, Out),

        {ok, Inp2} = gen_tcp:recv(Client, 14),
        ?assertEqual(<<"flush 1:foo:2\n">>, Inp2),
        gen_tcp:send(Client, <<"Done\n">>),

        {ok, Inp3} = gen_tcp:recv(Client, 15),
        ?assertEqual(<<"flush 0:test:1\n">>, Inp3),
        gen_tcp:send(Client, <<"Done\n">>)
    end),

    {reply, Resp, State} = br_vnode:handle_command({flush_filter, undefined},
                                                   undefined, State),
    ?assertEqual(done, Resp).

flush_specific_test() ->
    {_, _, Client, State} = new_vnode(0),

    % Simulate a response
    spawn_link(fun() ->
        {ok, Inp} = gen_tcp:recv(Client, 5),
        ?assertEqual(<<"list\n">>, Inp),
        Out = <<"START\n0:test:1 0.001 1000 500 200\n1:foo:2 0.001 1000 500 200\nEND\n">>,
        gen_tcp:send(Client, Out),

        {ok, Inp3} = gen_tcp:recv(Client, 15),
        ?assertEqual(<<"flush 0:test:1\n">>, Inp3),
        gen_tcp:send(Client, <<"Done\n">>)
    end),

    {reply, Resp, State} = br_vnode:handle_command({flush_filter, <<"test">>},
                                                   undefined, State),
    ?assertEqual(done, Resp).

flush_nofilt_test() ->
    {_, _, Client, State} = new_vnode(0),

    % Simulate a response
    spawn_link(fun() ->
        {ok, Inp} = gen_tcp:recv(Client, 5),
        ?assertEqual(<<"list\n">>, Inp),
        Out = <<"START\nEND\n">>,
        gen_tcp:send(Client, Out)
    end),

    {reply, Resp, State} = br_vnode:handle_command({flush_filter, <<"test">>},
                                                   undefined, State),
    ?assertEqual({error, no_filter}, Resp).


