-module(br_repair).
-behavior(gen_server).
-export([start_link/0, init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).
-export([maybe_repair/1]).

% This is a 'grace' period that we provide to
% wait for any in progress drop's or creates
% to complete. It prevents read repair from counter
% acting a user-issued drop or create. It is only
% a heuristic, and it is entirely possible that we
% get this wrong.
-define(GRACE_INTERVAL, 120000).  % 2 minutes

% Timeout interval for listing from the cluster
-define(LIST_TIMEOUT, 60000).  % 1 minute

% Used to count the number of reports of
% a filter existing or not existing
-record(counter, {exist=0, not_exist=0}).

% Used for our gen_server state
-record(state, {
        % Tracks the pending repairs
        pending,
        % Tracks the processes
        procs
}).

%%%
% Start API
%%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%
% gen_server API
%%%

init([]) ->
    S = #state{pending=dict:new(), procs=dict:new()},
    {ok, S}.

% Checks if a filter should be repaired by the
% caller. Returns true if so, and false if repair
% is already scheduled.
handle_call({should_repair, Filter, Caller}, _From, State) ->
    {Resp, NS} = case dict:is_key(Filter, State#state.pending) of
        % Repair taking place, ignore
        true -> {false, State};

        % No repair, should take place
        false ->
            % Monitor the caller
            MRef = monitor(process, Caller),

            % Mark as pending
            NewProcs = dict:store(Caller, {MRef, Filter}, State#state.procs),
            NewPend = dict:store(Filter, Caller, State#state.pending),

            % Update state
            {true, State#state{pending=NewPend, procs=NewProcs}}
    end,
    {reply, Resp, NS};

handle_call(Cmd, _From, State) ->
    lager:warning("Unexpected command: ~p", [Cmd]),
    {noreply, State}.

handle_cast(Msg, State) ->
    lager:warning("Unexpected message: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _, process, Pid, Reason}, State) ->
    % Log any bad deaths
    case Reason of
        normal -> ok;
        _ -> lager:warning("Repair FSM ~p died with bad reason ~p", [Pid, Reason])
    end,

    % Unschedule
    NS = case dict:find(Pid, State#state.procs) of
        {ok, {_MRef, Filter}} ->
            % Remove references
            NewProcs = dict:erase(Pid, State#state.procs),
            NewPend = dict:erase(Filter, State#state.pending),
            State#state{pending=NewPend, procs=NewProcs};

        error -> State
    end,
    {noreply, NS};

handle_info(Msg, State) ->
    lager:warning("Unexpected message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_Vsn, State, _Extra) -> {ok, State}.

%%%
% Public API
%%%

% This method is invoked when a filter might be inconsistent
% and requires repair. If by some query it appears that there
% is a slice missing or a phantom slice, we can detect this and
% issue a create or drop as appropriate. We do need to consider
% the possibility that filter is being dropped or created but that
% the operation is not yet completed.
maybe_repair(Filter) ->
    % Check if we should execute the repair
    DoRepair = gen_server:call(?MODULE, {should_repair, Filter, self()}),
    case DoRepair of
        true -> check_repair(Filter);
        _ -> ok
    end.


%%%
% Helper methods
%%%

% Checks if a repair is necessary
check_repair(Filter) ->
    % Wait for a grace period
    timer:sleep(?GRACE_INTERVAL),

    % List the slice info
    List = list_slices(Filter),

    % Get the expected number of partitions
    P = br_util:num_partitions(),

    % Count the number of slices
    case count_slices(List) of
        {error, timeout} ->
            lager:warning("Failed to perform cluster info on ~p for read repair!", [Filter]);

        % If more than half believe the filter does not exist but
        % some do believe it exists, we issue a drop
        #counter{exist=E, not_exist=NE} when NE > (P / 2), E > 0 ->
            lager:notice("Issuing drop as part of read repair for ~p!", [Filter]),
            bloomd_ring:drop(Filter);

        % The complement is that if more than half think it exists,
        % but some do not believe it exists, we issue a create
        #counter{exist=E, not_exist=NE} when E > (P / 2), NE > 0 ->
            Options = create_options(List),
            lager:notice("Issuing create as part of read repair for ~p with options ~p!",
                         [Filter, Options]),
            bloomd_ring:create(Filter, Options);

        % Log if there are slices that both exist and don't exist,
        % but not enough to come to a consensus decision
        #counter{exist=E, not_exist=NE} when E > 0, NE > 0 ->
            lager:warning("Not sure how to read repair filter ~p. Votes: Exist: ~p, Not Exist: ~p", [Filter, E, NE]);

        _ -> ok
    end.


% Lists the slices using a full cluster FSM
list_slices(Filter) ->
    % Start a full cluster list operation
    {ok, ReqId} = br_cluster_fsm:start_op(info, Filter),
    receive
        {ReqId, Resp} -> Resp
    after ?LIST_TIMEOUT ->
        {error, timeout}
    end.

% Counts the number of reports that a filter
% exists or does not exist
count_slices(List) ->
    case List of
        {error, timeout} -> {error, timeout};
        _ ->
            lists:foldl(fun(Resp, C=#counter{exist=E, not_exist=NE}) ->
                case Resp of
                    {ok, _} -> C#counter{exist=E+1};
                    {error, no_filter} -> C#counter{not_exist=NE+1};
                    _ -> C
                end
            end, #counter{}, List)
    end.

% Uses the results of list_slices to re-create the
% settings required for issuing a filter create
create_options(List) ->
    % Get the first okay response
    [First|_] = lists:filter(fun({ok,_}) -> true; (_) -> false end, List),
    {ok, [{_Slice, Info}|_]} = First,

    % Get the probability and capacity
    {probability, Prob} = lists:keyfind(probability, 1, Info),
    {capacity, SliceCapacity} = lists:keyfind(capacity, 1, Info),

    % True capacity is SliceCapacity * Slices
    Capacity = SliceCapacity * br_util:num_partitions(),

    % Return the filter capacity and probability
    [{capacity, Capacity}, {probability, Prob}].

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

create_options_test() ->
    % Mock the call to br_util:num_partitions
    M = em:new(),
    em:strict(M, br_util, num_partitions, [], {return, 64}),
    ok = em:replay(M),

    Inp = [{error, no_filter}, {ok, [{1, [{tubez, 100},
                                     {probability, 0.001},
                                     {checks, 100},
                                          {capacity, 10000}]}]}],
    Res = create_options(Inp),
    Expect = [{capacity, 640000}, {probability, 0.001}],

    em:verify(M),
    ?assertEqual(Expect, Res).

count_slices_test() ->
    Inp = [{ok, []}, {error, no_filter}, {error, command_failed}, {ok, []}],
    Out = count_slices(Inp),
    Expect = #counter{exist=2, not_exist=1},
    ?assertEqual(Expect, Out).

-endif.

