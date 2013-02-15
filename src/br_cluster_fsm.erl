-module(br_cluster_fsm).
-behavior(gen_fsm).
-export([start_link/3]).
-export([init/1, handle_info/3, handle_event/3, handle_sync_event/4,
         code_change/4, terminate/3]).
-export([prepare/2, executing/2, waiting/2]).


-record(state, {
        % Unique ID we send back to the client
        req_id,
        % Client PID to reply to
        from,
        % Operation
        op
        }).


%%%
% Start API
%%%

start_link(ReqId, From, Op) ->
    gen_fsm:start_link(?MODULE, [ReqId, From, Op], []).

%%%
% Gen FSM API
%%%

init([ReqId, From, Op]) ->
    State = #state{req_id=ReqId, from=From, op=Op},
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

prepare(_Event, State) ->
    {next_state, execute, State, 0}.

executing(_Event, State) ->
    {next_state, waiting, State, 0}.

waiting(_Event, State) ->
    {next_state, waiting, State, 0}.

