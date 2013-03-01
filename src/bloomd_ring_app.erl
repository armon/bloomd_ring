-module(bloomd_ring_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case bloomd_ring_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, br_vnode}]),
            ok = riak_core_node_watcher:service_up(bloomd, self()),

            EntryRoute = {["bloomd", "ping"], br_wm_ping, []},
            webmachine_router:add_route(EntryRoute),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
