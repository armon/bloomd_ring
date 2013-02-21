-module(br_coverage_fsm).
-export([start_link/4, start_op/2]).
-export([init/2, process_results/2, finish/2]).
-behaviour(riak_core_coverage_fsm).


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
        resp=[]
        }).

% This is the maximum time we wait
% before we timeout the request.
-define(WAIT_TIMEOUT, 60000).


%%%
% Start API
%%%

start_link(ReqId, From, Op, Args) ->
    riak_core_coverage_fsm:start_link(?MODULE, {pid, ReqId, From}, [ReqId, From, Op, Args], []).

start_op(Op, Args) ->
    ReqId = erlang:make_ref(),
    {ok, _} = br_coverage_fsm_sup:start_fsm([ReqId, self(), Op, Args]),
    {ok, ReqId}.


%%%
% riak_core_coverage_fsm API
%%%

-define(MASTER, br_vnode_master).
init(_, [ReqId, From, Op, Args]) ->
    State = #state{req_id=ReqId, from=From, op=Op, args=Args},
    Req = case Op of
        info -> {info_filter, Args};
        list -> list_filters
    end,
    {Req, allup, 3, 1, bloomd, ?MASTER, ?WAIT_TIMEOUT, State}.


process_results(Resp, State=#state{resp=Buf}) ->
    NewBuf = [Resp | Buf],
    {ok, State#state{resp=NewBuf}}.


finish(clean, #state{req_id=ReqId, from=From, resp=Buf}) ->
    From ! {ReqId, {ok, Buf}};

finish({error, timeout}, #state{req_id=ReqId, from=From}) ->
    lager:warning("Timed out waiting for all responses!"),
    From ! {ReqId, {error, timeout}};

finish(Reason, #state{req_id=ReqId, from=From}) ->
    lager:warning("Coverage query failed! Reason: ~p", [Reason]),
    From ! {ReqId, {error, Reason}}.

