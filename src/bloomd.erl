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
    {ok, []}.

%% @doc Drops a filter
drop(_Filter) ->
    ok.

%% @doc Closes a filter
close(_Filter) ->
    ok.

%% @doc Removes a filter from the bloomd internal manager
clear(_Filter) ->
    ok.

%% @doc Checks for a key in a filter
check(_Filter, _Key) ->
    ok.

%% @doc Checks for multiple keys in a filter
multi(_Filter, _Keys) ->
    ok.

%% @doc Sets a key in a filter
set(_Filter, _Key) ->
    ok.

%% @doc Sets multiple keys in a filter
bulk(_Filter, _Keys) ->
    ok.

%% @doc Gets information about a filter
info(_Filter, _Absolute) ->
    ok.

%% @doc Flushes either a given filter, or all filters.
flush(_Filter) ->
    ok.

