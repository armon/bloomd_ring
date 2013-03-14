-module(br_quorum_fsm_sup).
-behavior(supervisor).

-export([start_link/0, start_fsm/1]).
-export([init/1]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    % Get the size of the pool
    {ok, PoolSize} = application:get_env(bloomd_quorum_pool_size),

    % Start the worker pool
    PoolArgs = [{name, {local, quorum_pool}},
                {size, PoolSize},
                {worker_module, br_quorum_fsm},
                {max_overflow, 0}
               ],
    PoolSpec = poolboy:child_spec(quorum_pool, PoolArgs),

    {ok, {{one_for_one, 10, 10}, [PoolSpec]}}.

start_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

