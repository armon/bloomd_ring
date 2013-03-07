% This module is used to perform work
% asyncronously ouside of the vnode. It is
% important for tasks that would otherwise block.
-module(br_worker).
-behaviour(riak_core_vnode_worker).
-export([init_worker/3,
         handle_work/3]).

-record(state, {index}).

% Initializes a worker
init_worker(VNodeIndex, _Args, _Props) ->
     {ok, #state{index=VNodeIndex}}.

handle_work({handoff, Idx, Fun, Accum}, _Sender, State) ->
    lager:notice("Attempting handoff of index: ~p", [Idx]),
    Resp = br_handoff:handoff(Idx, Fun, Accum),
    {reply, Resp, State}.

