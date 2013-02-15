-module(br_cluster_fsm_sup).
-behavior(supervisor).

-export([start_link/0, start_fsm/1]).
-export([init/1]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    ClusterFSM = {undefined,
                  {br_cluster_fsm, start_link, []},
                  temporary, 5000, worker, [br_cluster_fsm]},

    {ok, {{simple_one_for_one, 10, 10}, [ClusterFSM]}}.

start_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

