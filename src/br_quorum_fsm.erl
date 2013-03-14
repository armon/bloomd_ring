-module(br_quorum_fsm).
-behavior(gen_fsm).
-export([start_link/1, start_op/2]).
-export([init/1, handle_info/3, handle_event/3, handle_sync_event/4,
         code_change/4, terminate/3]).
-export([initialize/2, waiting/2, repairing/2]).


-record(state, {
        % Unique ID we send back to the client
        req_id,
        % Client PID to reply to
        from,
        % Operation
        op,
        % Operation args
        args,
        % Preflsit
        preflist,
        % Responses
        resp=[],
        % Have we responded
        responded=false
        }).

% This is the maximum time we wait
% before we timeout the request.
-define(WAIT_TIMEOUT, 60000).

% This is the replication factor
-define(N, 3).

% This is our R/W number. We wait for
% this many responses before responding
% to the client.
-define(RW, 2).

% Vnode to talk to
-define(MASTER, br_vnode_master).

% Milliseconds to wait for an available quorum FSM
-define(BLOCK_TIME, 5000).

%%%
% Start API
%%%

start_link(Args) ->
    gen_fsm:start_link(?MODULE, [], []).

start_op(Op, Args) ->
    Pid = poolboy:checkout(quorum_pool, true, ?BLOCK_TIME),
    ReqId = erlang:make_ref(),
    ok = gen_fsm:send_event(Pid, {init, ReqId, self(), Op, Args}),
    {ok, ReqId}.


%%%
% Gen FSM API
%%%

init([]) ->
    {ok, initialize, undefined, hibernate}.

handle_info(Info, StateName, State) ->
    lager:warning("Unexpected message: ~p", [Info]),
    {next_state, StateName, State, 0}.

handle_event(Event, StateName, State) ->
    lager:warning("Unexpected event: ~p", [Event]),
    {next_state, StateName, State, 0}.

handle_sync_event(Event, _From, StateName, State) ->
    lager:warning("Unexpected event: ~p", [Event]),
    {next_state, StateName, State, 0}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.


%%%
% FSM States
%%%

initialize({init, ReqId, From, Op, Args={FilterName, Key}}, _State) ->
    % Get the ring
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),

    % Determine the slice
    P = riak_core_ring:num_partitions(Ring),
    Slice = br_util:keyslice(Key, P),

    % Get the preflist
    DocIdx = br_util:hash_slice(FilterName, Slice),
    {Preflist, _} = lists:split(?N, riak_core_ring:preflist(DocIdx, Ring)),

    % Get the command
    Cmd = case Op of
        check -> {check_filter, FilterName, Slice, Key};
        set -> {set_filter, FilterName, Slice, Key}
    end,
    riak_core_vnode_master:command(Preflist, Cmd, {fsm, undefined, self()}, ?MASTER),

    % Create the state and wait
    State = #state{req_id=ReqId, from=From, op=Op, args=Args, preflist=Preflist},
    {next_state, waiting, State, ?WAIT_TIMEOUT}.

waiting(timeout, State=#state{req_id=ReqId, from=From}) ->
    lager:warning("Timed out waiting for all responses!"),
    From ! {ReqId, {error, timeout}},
    {next_state, repairing, State, 0};

waiting(Resp, State=#state{preflist=Pref, resp=Buf, from=From, req_id=ReqId}) ->
    N = length(Pref),
    NewBuf = [Resp | Buf],
    NumResp = length(NewBuf),
    NewState = State#state{resp=NewBuf},
    case NumResp of
        % If we have all the responses, go to repair
        N ->
            % Respond with the most common response if we didn't respond
            % already
            NS = case State#state.responded of
                false ->
                    [{_, Common}|_] = response_counts(NewBuf),
                    From ! {ReqId, Common},
                    NewState#state{responded=true};

                _ -> State
            end,
            {next_state, repairing, NS, 0};

        % At R/W we may be able to respond if we have consensus
        ?RW ->
            % If we have consensus, we can reply now
            NS = case consensus(NewBuf) of
                {true, V} ->
                    From ! {ReqId, V},
                    NewState#state{responded=true};

                % No consensus, wait for the final replies
                {false, _} -> NewState
            end,
            {next_state, waiting, NS, ?WAIT_TIMEOUT};

        % Still waiting
        _ -> {next_state, waiting, NewState, ?WAIT_TIMEOUT}
    end.


% Handles doing a read-repair after we've responded to the client
repairing(timeout, State=#state{resp=Resps, op=Op, args={Filter, Key}}) ->
    % Count all the responses
    Counted = response_counts(Resps),
    case Counted of
        % We don't do anything if all nodes agree
        [_Agreed] -> ok;

        % If we are doing a `check` and 2 nodes belive
        % a key exists, perform a set to repair the 3rd node.
        % Since bloomd cannot 'unset' there is no way to repair
        % a 2 No / 1 Yes situation
        [{2, {ok, true}}, {1, {ok, false}}] when Op =:= check ->
            bloomd_ring:set(Filter, Key);

        % If one of the nodes belives that the filter exists
        % then maybe a drop is needed
        [{2, {error, no_filter}}, {1, {ok, _}}] ->
            br_repair:maybe_repair(Filter);

        % If one of the nodes belive that the filter does not
        % exist, then maybe a create is needed. Handle the
        % non-agreed case
        [{1, {ok, _}}, {1, {ok, _}}, {1, {error, no_filter}}] ->
            br_repair:maybe_repair(Filter);

        % If one of the nodes belive that the filter does not
        % exist, then maybe a create is needed
        [{2, {ok, _}}, {1, {error, no_filter}}] ->
            br_repair:maybe_repair(Filter);

        % If we don't match any condition, do nothing
        _ -> ok
    end,

    % Check into the pool
    poolboy:checkin(quorum_pool, self()),
    {next_state, initialize, undefined, hibernate}.


%%%
% Helpers
%%%

% Aggregates the responses and returns a list
% of [{Count, Response}].
-spec response_counts([term()]) -> [{integer(), term()}].
response_counts(Responses) ->
    Counted = count_occurences(lists:sort(Responses), []),
    lists:reverse(lists:sort(Counted)).

% Counts the occurences of a term
count_occurences([Head | Tail], [{Count, Head} | Acc]) ->
    count_occurences(Tail, [{Count+1, Head} | Acc]);
count_occurences([Head | Tail], Acc) ->
    count_occurences(Tail, [{1, Head} | Acc]);
count_occurences([], Acc) ->
    Acc.

% Checks for consensus
% Returns {Have Consensus, Value}
% If Have Consensus, then value is the consensus response
% Otherwise it is the return value of response_counts
-spec consensus([term()]) -> {boolean(), term()}.
consensus(Responses) ->
    % Get the response counts
    Counted = response_counts(Responses),
    case Counted of
        % All agree on one response
        [{_, Resp}] -> {true, Resp};

        % At least R/W agree on a response
        [{C, Resp}|_] when C >= ?RW -> {true, Resp};

        % No consensus
        _ -> {false, Counted}
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

response_counts_test() ->
    R = [ok, ok, true, false, {error, bad}, {error, bad}, {error, bad}],
    C = response_counts(R),
    ?assertEqual([{3, {error, bad}}, {2, ok}, {1, true}, {1, false}], C).

consensus_rw_test() ->
    R = [ok, ok, true, false, {error, bad}, {error, bad}, {error, bad}],
    C = consensus(R),
    ?assertEqual({true, {error, bad}}, C).

consensus_all_test() ->
    R = [ok, ok, ok],
    C = consensus(R),
    ?assertEqual({true, ok}, C).

consensus_equal_test() ->
    R = [true, false, {error, foobar}],
    C = consensus(R),
    ?assertEqual({false, [{1, {error, foobar}}, {1, true}, {1, false}]}, C).

-endif.

