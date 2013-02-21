-module(bloomd_ring_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    % Collect all the variables first
    {ok, Port} = application:get_env(bloomd_ascii_port),
    {ok, AcceptPool} = application:get_env(bloomd_accept_pool),

    % Connection Manager
    ConnManager = {conn_manager,
           {br_conn_manager_sup, start_link, []},
           permanent, 60000, supervisor, dynamic},

    % Accept Manager, needs port and pool size
    AcceptManager  = {acceptors,
           {br_acceptor_sup, start_link, [Port, AcceptPool]},
           permanent, 60000, supervisor, dynamic},


    VMaster = { bloomd_vnode_master,
                  {riak_core_vnode_master, start_link, [br_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},

    % FSM Managers
    ClusterFSM = {br_cluster_fsm_sup,
                  {br_cluster_fsm_sup, start_link, []},
                  permanent, 60000, supervisor, dynamic},

    CoverageFSM = {br_coverage_fsm_sup,
                  {br_coverage_fsm_sup, start_link, []},
                  permanent, 60000, supervisor, dynamic},

    QuorumFSM = {br_quorum_fsm_sup,
                  {br_quorum_fsm_sup, start_link, []},
                  permanent, 60000, supervisor, dynamic},

    { ok,
        { {one_for_one, 5, 10},
          [ConnManager, AcceptManager, VMaster,
           ClusterFSM, CoverageFSM, QuorumFSM]}}.

