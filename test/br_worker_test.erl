-module(br_worker_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

new_worker() -> new_worker(0).
new_worker(Idx) ->
    {ok, State} = br_worker:init_worker(Idx, [], undefined),
    State.

handoff_test() ->
    % Get new worker
    State = new_worker(5),

    % Verify handoff is triggered
    M = em:new(),
    em:strict(M, br_handoff, handoff, [5, foldfun, foldacc], {return, ok}),
    ok = em:replay(M),

    Resp = br_worker:handle_work({handoff, 5, foldfun, foldacc}, undefined, State),
    ?assertEqual({reply, ok, State}, Resp),
    em:verify(M).

