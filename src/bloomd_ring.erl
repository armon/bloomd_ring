-module(bloomd).
-include("bloomd.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([ping/0, create/2, list/0, drop/1, close/1, clear/1,
        check/2, multi/2, set/2, bulk/2, info/2, flush/1]).

%% Public API

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, bloomd),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, bloomd_vnode_master).

%% @doc Creates a new filter with the given name and options list
create(_Filter, _OptionsList) ->
    ok.

%% @doc Lists all the existing filters using a covering set query
list() ->
    lager:info("Info called"),
    {ok, []}.

%% @doc Drops a filter
drop(Filter) ->
    lager:info("Drop called on: ~p", [Filter]),
    ok.

%% @doc Closes a filter
close(Filter) ->
    lager:info("Close called on: ~p", [Filter]),
    ok.

%% @doc Removes a filter from the bloomd internal manager
clear(Filter) ->
    lager:info("Clear called on: ~p", [Filter]),
    ok.

%% @doc Checks for a key in a filter
check(Filter, Key) ->
    lager:info("Check called on: ~p for: ~p", [Filter, Key]),
    ok.

%% @doc Checks for multiple keys in a filter
multi(Filter, Keys) ->
    lager:info("Multi called on: ~p for: ~p", [Filter, Keys]),
    ok.

%% @doc Sets a key in a filter
set(Filter, Key) ->
    lager:info("Set called on: ~p for: ~p", [Filter, Key]),
    ok.

%% @doc Sets multiple keys in a filter
bulk(Filter, Keys) ->
    lager:info("Bulk called on: ~p for: ~p", [Filter, Keys]),
    ok.

%% @doc Gets information about a filter
info(Filter, Absolute) ->
    lager:info("Info called on: ~p with absolute: ~p", [Filter, Absolute]),
    ok.

%% @doc Flushes either a given filter, or all filters.
flush(Filter) ->
    lager:info("Flush called on: ~p", [Filter]),
    ok.

