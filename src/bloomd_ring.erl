-module(bloomd_ring).
-include("bloomd_ring.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([ping/0, create/2, list/1, drop/1, close/1, clear/1,
        check/2, multi/2, set/2, bulk/2, info/2, flush/1]).

% Only for pmap
-export([pmap_cmd/3]).

-define(SHORT_WAIT, 15000).
-define(DEFAULT_TIMEOUT, 30000).
-define(LONG_WAIT, 60000).
-define(EXTREME_WAIT, 300000).

% This is the minimum partition size after we divide.
% If a partition is too small, it will scale poorly
-define(MIN_PARTITION_SIZE, 16384).

%% Public API

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, bloomd),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, br_vnode_master).


%% @doc Creates a new filter with the given name and options list
create(Filter, OptionsList) ->
    lager:info("Create called on: ~p with: ~p", [Filter, OptionsList]),

    % If in-memory 1 is specified, we drop the option silently
    Opt1 = case proplists:get_value(in_memory, OptionsList, 0) of
        1 -> lists:keydelete(in_memory, 1, OptionsList);
        _ -> OptionsList
    end,

    % If an initial capacity is provided, we silently replace it
    % with Capacity / Partitions, since underneath the hood we
    % are slicing the data into many partitions.
    Opt2 = case proplists:get_value(capacity, Opt1) of
        undefined -> Opt1;
        Capacity ->
            Partitions = br_util:num_partitions(),
            NewCap = max(br_util:ceiling(Capacity / Partitions), ?MIN_PARTITION_SIZE),
            lists:keyreplace(capacity, 1, Opt1, {capacity, NewCap})
    end,

    % Start a full cluster FSM to send out the command
    {ok, ReqId} = br_cluster_fsm:start_op(create, {Filter, Opt2}),

    % Wait for 60 seconds for the request to complete
    Resp = wait_for_req(ReqId, ?LONG_WAIT),
    case Resp of
        {error, timeout} ->
            lager:warning("Timed out waiting for the nodes to create filter!"),
            {error, timeout};

        {ok, Results} ->
            case lists:all(fun(R) -> R =:= exists end, Results) of
                true -> {ok, exists};
                _ -> case lists:all(fun(R) -> R =:= done orelse R =:= exists end, Results) of
                        true -> {ok, done};
                        _ ->
                            % Check for a client error
                            case lists:all(fun({error, {client_error, _}}) -> true; (_) -> false end, Results) of
                                true -> [Err|_] = Results, Err;
                                _ ->
                                    lager:warning("Nodes did not agree on create of ~p. Responses: ~p", [Filter, Results]),
                                    {error, internal_error}
                            end
                    end
            end
    end.


%% @doc Lists all the existing filters using a covering set query
list(Absolute) ->
    % DEBUG
    % lager:info("List called. Absolute: ~p", [Absolute]),

    % Start a full cluster FSM to send out the command
    {ok, ReqId} = case Absolute of
        true -> br_cluster_fsm:start_op(list, undefined);
        false -> br_coverage_fsm:start_op(list, undefined)
    end,

    % Wait default interval for the request to complete
    Resp = wait_for_req(ReqId),
    case Resp of
        {error, timeout} ->
            lager:warning("Timed out waiting for the nodes to list filters!"),
            {error, timeout};

        {ok, Results} ->
            {_Errors, Valid} = lists:partition(fun({error, _}) -> true; (_) -> false end, Results),
            case Valid of
                [] ->
                    lager:warning("Nodes failed to list. Responses: ~p", [Results]),
                    {error, internal_error};
                _ ->
                    % Flatten all the result info
                    FilterInfo = lists:append([I || {ok, I} <- Valid]),

                    % Merge and reconcile the values
                    Merged = br_util:collapse_list_info(FilterInfo),
                    {ok, Merged}
            end
    end.


%% @doc Drops a filter
drop(Filter) ->
    lager:info("Drop called on: ~p", [Filter]),

    % Start a full cluster FSM to send out the command
    {ok, ReqId} = br_cluster_fsm:start_op(drop, Filter),

    % Wait default interval for the request to complete
    Resp = wait_for_req(ReqId),
    case Resp of
        {error, timeout} ->
            lager:warning("Timed out waiting for the nodes to drop filter!"),
            {error, timeout};

        {ok, Results} ->
            case lists:all(fun(R) -> R =:= {error, no_filter} end, Results) of
                true -> {error, no_filter};
                _ -> case lists:all(fun(R) -> R =:= done orelse R =:= {error, no_filter} end, Results) of
                        true -> {ok, done};
                        _ ->
                            lager:warning("Nodes did not agree on drop of ~p. Responses: ~p", [Filter, Results]),
                            {error, internal_error}
                    end
            end
    end.


%% @doc Closes a filter
close(Filter) ->
    lager:info("Close called on: ~p", [Filter]),

    % Start a full cluster FSM to send out the command
    {ok, ReqId} = br_cluster_fsm:start_op(close, Filter),

    % Wait default interval for the request to complete
    Resp = wait_for_req(ReqId, ?LONG_WAIT),
    case Resp of
        {error, timeout} ->
            lager:warning("Timed out waiting for the nodes to close the filter!"),
            {error, timeout};

        {ok, Results} ->
            case lists:all(fun(R) -> R =:= {error, no_filter} end, Results) of
                true -> {error, no_filter};
                _ -> case lists:all(fun(R) -> R =:= done orelse R =:= {error, no_filter} end, Results) of
                        true -> {ok, done};
                        _ ->
                            lager:warning("Nodes did not agree on close of ~p. Responses: ~p", [Filter, Results]),
                            {error, internal_error}
                    end
            end
    end.


%% @doc Removes a filter from the bloomd internal manager
clear(Filter) ->
    lager:info("Clear called on: ~p", [Filter]),

    % Start a full cluster FSM to send out the command
    {ok, ReqId} = br_cluster_fsm:start_op(clear, Filter),

    % Wait default interval for the request to complete
    Resp = wait_for_req(ReqId),
    case Resp of
        {error, timeout} ->
            lager:warning("Timed out waiting for the nodes to clear the filter!"),
            {error, timeout};

        {ok, Results} ->
            case lists:any(fun(R) -> R =:= {error, not_proxied} end, Results) of
                true -> {error, not_proxied};
                _ ->
                    case lists:all(fun(R) -> R =:= {error, no_filter} end, Results) of
                        true -> {error, no_filter};
                        _ -> case lists:all(fun(R) -> R =:= done orelse R =:= {error, no_filter} end, Results) of
                                true -> {ok, done};
                                _ ->
                                    lager:warning("Nodes did not agree on clear of ~p. Responses: ~p",
                                                  [Filter, Results]),
                                    {error, internal_error}
                            end
                    end
            end
    end.


%% @doc Checks for a key in a filter
check(Filter, Key) ->
    % DEBUG
    %lager:info("Check called on: ~p for: ~p", [Filter, Key]),

    % Start a quorum FSM to send out the command
    {ok, ReqId} = br_quorum_fsm:start_op(check, {Filter, Key}),

    % Short wait interval for the request to complete
    Resp = wait_for_req(ReqId, ?SHORT_WAIT),
    case Resp of
        {ok, Result} -> {ok, Result};
        {error, ErrType} ->
            Err = case ErrType of
                % Ignore expected errors
                no_filter -> no_filter;
                timeout ->
                    lager:warning("Timed out waiting for the nodes to check the filter!"),
                    ErrType;
                _ ->
                    lager:warning("Nodes failed to check. Response: ~p", [Resp]),
                    internal_error
            end,
            {error, Err}
    end.


%% @doc Checks for multiple keys in a filter
multi(Filter, Keys) ->
    % DEBUG
    % lager:info("Multi called on: ~p for: ~p", [Filter, Keys]),

    % Execute all the checks in parallel
    Results = rpc:pmap({bloomd_ring, pmap_cmd}, [check, [Filter]], Keys),

    % Return the first error result if any
    {Errs, Resp} = lists:partition(fun({error, _}) -> true; (_) -> false end, Results),
    case Errs of
        [Err|_] -> Err;
        [] ->
            % Repackage the okay results
            KeyChecks = [C || {ok, C} <- Resp],
            {ok, KeyChecks}
    end.


%% @doc Sets a key in a filter
set(Filter, Key) ->
    % DEBUG
    %lager:info("Set called on: ~p for: ~p", [Filter, Key]),

    % Start a quorum FSM to send out the command
    {ok, ReqId} = br_quorum_fsm:start_op(set, {Filter, Key}),

    % Short wait interval for the request to complete
    Resp = wait_for_req(ReqId, ?SHORT_WAIT),
    case Resp of
        {ok, Result} -> {ok, Result};
        {error, ErrType} ->
            Err = case ErrType of
                % Ignore expected errors
                no_filter -> no_filter;
                timeout ->
                    lager:warning("Timed out waiting for the nodes to set the filter!"),
                    ErrType;
                _ ->
                    lager:warning("Nodes failed to set. Response: ~p", [Resp]),
                    internal_error
            end,
            {error, Err}
    end.


%% @doc Sets multiple keys in a filter
bulk(Filter, Keys) ->
    % DEBUG
    % lager:info("Bulk called on: ~p for: ~p", [Filter, Keys]),

    % Execute all the sets in parallel
    Results = rpc:pmap({bloomd_ring, pmap_cmd}, [set, [Filter]], Keys),

    % Return the first error result if any
    {Errs, Resp} = lists:partition(fun({error, _}) -> true; (_) -> false end, Results),
    case Errs of
        [Err|_] -> Err;
        [] ->
            % Repackage the okay results
            KeyChecks = [C || {ok, C} <- Resp],
            {ok, KeyChecks}
    end.


%% @doc Gets information about a filter
info(Filter, Absolute) ->
    % DEBUG
    % lager:info("Info called on: ~p with absolute: ~p", [Filter, Absolute]),

    % Start an FSM to send out the command
    {ok, ReqId} = case Absolute of
        true -> br_cluster_fsm:start_op(info, Filter);
        false -> br_coverage_fsm:start_op(info, Filter)
    end,

    % Wait default interval for the request to complete
    Resp = wait_for_req(ReqId),
    case Resp of
        {error, timeout} ->
            lager:warning("Timed out waiting for the nodes to get filter info!"),
            {error, timeout};

        {ok, Results} ->
            {_Errors, Valid} = lists:partition(fun({error, _}) -> true; (_) -> false end, Results),
            case Valid of
                [] ->
                    case lists:all(fun({error, no_filter}) -> true; (_) -> false end, Results) of
                        true -> {error, no_filter};
                        _ ->
                            lager:warning("Nodes failed to get info. Responses: ~p", [Results]),
                            {error, internal_error}
                    end;
                _ ->
                    % Flatten all the result info
                    FilterInfo = lists:append([I || {ok, I} <- Valid]),

                    % Merge and reconcile the values
                    Merged = br_util:merge_filter_info(FilterInfo),
                    {ok, Merged}
            end
    end.


%% @doc Flushes either a given filter, or all filters.
flush(Filter) ->
    lager:info("Flush called on: ~p", [Filter]),

    % Start a full cluster FSM to send out the command
    {ok, ReqId} = br_cluster_fsm:start_op(flush, Filter),

    % Wait default interval for the request to complete
    Resp = wait_for_req(ReqId, ?EXTREME_WAIT),
    case Resp of
        {error, timeout} ->
            lager:warning("Timed out waiting for the nodes to flush!"),
            {error, timeout};

        {ok, Results} ->
            case lists:all(fun(R) -> R =:= {error, no_filter} end, Results) of
                true -> {error, no_filter};
                _ -> case lists:all(fun(R) -> R =:= done orelse R =:= {error, no_filter} end, Results) of
                        true -> {ok, done};
                        _ ->
                            lager:warning("Nodes did not agree on flush of ~p. Responses: ~p", [Filter, Results]),
                            {error, internal_error}
                    end
            end
    end.


%%%
% Helper methods
%%%


% Waits for a specified request ID to return. Either
% waits for the default timeout, or a user specified one.
wait_for_req(ReqId) ->
    wait_for_req(ReqId, ?DEFAULT_TIMEOUT).
wait_for_req(ReqId, Timeout) ->
    receive
        {ReqId, Resp} -> Resp
    after Timeout ->
        {error, timeout}
    end.

% Helper method to be used with pmap. Moves the first
% argument to the end of the argument array.
pmap_cmd(Key, Op, Args) ->
    apply(bloomd_ring, Op, Args ++ [Key]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

wait_for_req_test() ->
    ReqId = 42,
    S = self(),
    spawn(fun() ->
        S ! {ReqId, ok}
    end),
    Res = wait_for_req(ReqId),
    ?assertEqual(ok, Res).

wait_for_req_timeout_test() ->
    ?assertEqual({error, timeout}, wait_for_req(os:timestamp(), 50)).

-endif.

