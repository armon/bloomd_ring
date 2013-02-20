-module(br_cluster_fsm).
-behavior(gen_fsm).
-export([start_link/4, start_op/2]).
-export([init/1, handle_info/3, handle_event/3, handle_sync_event/4,
         code_change/4, terminate/3]).
-export([prepare/2, executing/2, waiting/2]).


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
    gen_fsm:start_link(?MODULE, [ReqId, From, Op, Args], []).

start_op(Op, Args) ->
    ReqId = erlang:make_ref(),
    {ok, _} = br_cluster_fsm_sup:start_fsm([ReqId, self(), Op, Args]),
    {ok, ReqId}.


%%%
% Gen FSM API
%%%

init([ReqId, From, Op, Args]) ->
    State = #state{req_id=ReqId, from=From, op=Op, args=Args},
    {ok, prepare, State, 0}.

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

% Prepares for execution, gets the preflist ready
prepare(timeout, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Preflist = riak_core_ring:all_owners(Ring),
    {next_state, executing, State#state{preflist=Preflist}, 0}.


-define(MASTER, br_vnode_master).
executing(timeout, State=#state{preflist=Pref, op=Op, args=Args}) ->
    Cmd = case Op of
        create ->
            {Filter, Options} = Args,
            {create_filter, Filter, Options};
        drop -> {drop_filter, Args};
        close -> {close_filter, Args};
        clear -> {clear_filter, Args};
        flush -> {flush_filter, Args};
        info -> {info_filter, Args};
        list -> list_filters
    end,
    riak_core_vnode_master:command(Pref, Cmd, {fsm, undefined, self()}, ?MASTER),
    {next_state, waiting, State, ?WAIT_TIMEOUT}.


waiting(timeout, State=#state{req_id=ReqId, from=From}) ->
    lager:warning("Timed out waiting for all responses!"),
    From ! {ReqId, {error, timeout}},
    {stop, normal, State};

waiting(Resp, State=#state{preflist=Pref, resp=Buf}) ->
    NewBuf = [Resp | Buf],
    NumResp = length(NewBuf),
    Expect = length(Pref),
    case NumResp of
        % Have all the responses, we are done
        Expect ->
            #state{from=From, req_id=ReqId} = State,
            From ! {ReqId, {ok, NewBuf}},
            {stop, normal, State};

        % Still waiting
        _ -> {next_state, waiting, State#state{resp=NewBuf}, ?WAIT_TIMEOUT}
    end.

