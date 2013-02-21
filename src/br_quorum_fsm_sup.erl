-module(br_quorum_fsm_sup).
-behavior(supervisor).

-export([start_link/0, start_fsm/1]).
-export([init/1]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    QuorumFSM = {undefined,
                  {br_quorum_fsm, start_link, []},
                  temporary, 5000, worker, [br_quorum_fsm]},

    {ok, {{simple_one_for_one, 10, 10}, [QuorumFSM]}}.

start_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

