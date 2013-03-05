-module(br_util).
-export([ceiling/1,
         num_partitions/0,
         keyslice/1,
         keyslice/2,
         merge_slice_info/1,
         collapse_slice_info/1,
         merge_filter_info/1,
         hash_slice/2,
         collapse_list_info/1
        ]).

% Limit of phash2, 2^32
-define(HASH_LIMIT, 4294967296).

% Implements the mathematical ceiling operator
ceiling(X) when X < 0 ->
    trunc(X);
ceiling(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T + 1
    end.

% Returns the number of partitions
num_partitions() ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:num_partitions(R).


% Gets the slice for a given key
keyslice(Key) ->
    P = num_partitions(),
    keyslice(Key, P).


% Given a key and the number of partitions, returns which
% slice owns the given key
keyslice(Key, Partitions) when is_binary(Key), is_integer(Partitions) ->
    % Just use erlang:phash2 for distribution.
    % We want something that is fast and reasonably distributed
    Digest = erlang:phash2(Key, ?HASH_LIMIT),

    % Mod by the ring size
    Digest rem Partitions.


% Computes the index of the filter and slice on the ring
hash_slice(Filter, Slice) ->
    riak_core_util:chash_std_keyfun({Filter, Slice}).

% Merges the slice information returned by the `list` command for
% a single slice. Slice information is merged by taking the maximum
% of each value.
-spec merge_slice_info([Info :: list()]) -> list().
merge_slice_info(SliceInfo) ->
    Merged = lists:foldl(fun(Info, Accum) ->
        dict:merge(fun(_K, V1, V2) ->
            case V1 >= V2 of
                true -> V1;
                _ -> V2
            end
        end, Accum, dict:from_list(Info))
    end, dict:new(), SliceInfo),
    dict:to_list(Merged).

% Collapses and merges all the slices and their info.
% This is done by first grouping all the data by slice,
% and then merging the data for each slice.
-spec collapse_slice_info([{Slice :: term(), Info :: list()}]) -> [{Slice :: term(), Info :: list()}].
collapse_slice_info(SliceInfo) ->
    % Make a map of the slice to a list of all the info's
    Combined = lists:foldl(fun({Slice, Info}, Accum) ->
        dict:append(Slice, Info, Accum)
    end, dict:new(), SliceInfo),

    % Merge the slice info
    Merged = dict:map(fun(_K, V) ->
        merge_slice_info(V)
    end, Combined),

    % Convert back to a list
    dict:to_list(Merged).

% Merges the information about a single filter into
% a single info property list. This is done by collapsing the
% slice information, and then taking the sum of each slice property
% except for probability, which uses the maximum.
-spec merge_filter_info([{Slice :: term(), Info:: list()}]) -> list().
merge_filter_info(FilterInfo) ->
    % Collapse the slice info first
    SliceInfo = collapse_slice_info(FilterInfo),

    % Merge the slices together. We sum everything other
    % than probability.
    Merged = lists:foldl(fun({_Slice, Info}, Accum) ->
        dict:merge(fun(K, V1, V2) ->
            case K of
                probability ->
                    case V1 >= V2 of
                        true -> V1;
                        _ -> V2
                    end;
                _ -> V1 + V2
            end
        end, Accum, dict:from_list(Info))
    end, dict:new(), SliceInfo),

    % Convert back to list
    dict:to_list(Merged).

% Collapses the information from a list operation.
% First we group the data by filter, and then merge
% all the filter information.
collapse_list_info(ListInfo) ->
    % Group by filter first
    Grouped = lists:foldl(fun({Filter, Slices}, Accum) ->
        dict:append_list(Filter, Slices, Accum)
    end, dict:new(), ListInfo),

    % Merge all the filter info down
    Merged = dict:map(fun(_K, V) ->
        merge_filter_info(V)
    end, Grouped),

    % Convert back to list
    dict:to_list(Merged).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

ceiling_test() ->
    ?assertEqual(-1, ceiling(-1.1)),
    ?assertEqual(0, ceiling(-0.1)),
    ?assertEqual(1, ceiling(0.1)),
    ?assertEqual(3, ceiling(3)),
    ?assertEqual(5, ceiling(4.1)).

num_partitions_test() ->
    % Mock the call to riak_core_ring_manager:get_my_ring
    Ring = riak_core_ring:fresh(128, tubez),
    M = em:new(),
    em:strict(M, riak_core_ring_manager, get_my_ring, [], {return, {ok, Ring}}),
    ok = em:replay(M),

    ?assertEqual(128, num_partitions()),
    em:verify(M).

keyslice_1_test() ->
    % Mock the call to riak_core_ring_manager:get_my_ring
    Ring = riak_core_ring:fresh(64, tubez),
    M = em:new(),
    em:strict(M, riak_core_ring_manager, get_my_ring, [], {return, {ok, Ring}}),
    ok = em:replay(M),

    ?assertEqual(22, keyslice(<<"bar">>)),
    em:verify(M).

keyslice_2_test() ->
    ?assertEqual(0, keyslice(<<"foo">>, 1)),
    ?assertEqual(0, keyslice(<<"bar">>, 1)),
    ?assertEqual(0, keyslice(<<"baz">>, 1)),
    ?assertEqual(56, keyslice(<<"foo">>, 64)),
    ?assertEqual(22, keyslice(<<"bar">>, 64)),
    ?assertEqual(39, keyslice(<<"baz">>, 64)).

merge_slice_info_test() ->
    Info1 = [{probability, 0.001}, {bytes, 100}, {capacity, 1000}, {size, 2000}],
    Info2 = [{probability, 0.001}, {bytes, 200}, {capacity, 2000}, {size, 3000}],
    Info3 = [{probability, 0.002}, {bytes, 100}, {capacity, 1000}, {size, 4000}],
    Merged = merge_slice_info([Info1, Info2, Info3]),
    Expect = [{bytes, 200}, {size, 4000}, {capacity, 2000}, {probability, 0.002}],
    Expect = Merged.

collapse_slice_info_test() ->
    Info1 = {1, [{probability, 0.001}, {bytes, 100}, {capacity, 1000}, {size, 2000}]},
    Info2 = {1, [{probability, 0.002}, {bytes, 200}, {capacity, 2000}, {size, 3000}]},
    Info3 = {2, [{probability, 0.001}, {bytes, 100}, {capacity, 1000}, {size, 4000}]},
    Info4 = {2, [{probability, 0.005}, {bytes, 150}, {capacity, 4000}, {size, 2000}]},
    Merged = collapse_slice_info([Info1, Info2, Info3, Info4]),
    Expect = [
            {2, [{bytes, 150}, {size, 4000}, {capacity, 4000}, {probability, 0.005}]},
            {1, [{bytes, 200}, {size, 3000}, {capacity, 2000}, {probability, 0.002}]}
             ],
    ?assertEqual(Expect, Merged).

merge_filter_info_test() ->
    Info1 = {1, [{probability, 0.001}, {bytes, 100}, {capacity, 1000}, {size, 2000}]},
    Info2 = {1, [{probability, 0.002}, {bytes, 200}, {capacity, 2000}, {size, 3000}]},
    Info3 = {2, [{probability, 0.001}, {bytes, 100}, {capacity, 1000}, {size, 4000}]},
    Info4 = {2, [{probability, 0.005}, {bytes, 150}, {capacity, 4000}, {size, 2000}]},
    Merged = merge_filter_info([Info1, Info2, Info3, Info4]),
    Expect = [{bytes, 350}, {size, 7000}, {probability, 0.005}, {capacity, 6000}],
    ?assertEqual(Expect, Merged).

collapse_list_info_test() ->
    Info1 = {<<"Test">>, [
                {1, [{probability, 0.001}, {bytes, 100}, {capacity, 1000}, {size, 2000}]},
                {2, [{probability, 0.001}, {bytes, 100}, {capacity, 1000}, {size, 4000}]}
            ]},
    Info2 = {<<"Test">>, [
                {1, [{probability, 0.002}, {bytes, 200}, {capacity, 2000}, {size, 3000}]},
                {2, [{probability, 0.005}, {bytes, 150}, {capacity, 4000}, {size, 2000}]}
            ]},
    Info3 = {<<"Foo">>, [
                {3, [{probability, 0.001}, {bytes, 100}, {capacity, 1000}, {size, 2000}]},
                {4, [{probability, 0.001}, {bytes, 100}, {capacity, 1000}, {size, 4000}]}
            ]},
    Info4 = {<<"Foo">>, [
                {3, [{probability, 0.002}, {bytes, 200}, {capacity, 2000}, {size, 3000}]},
                {4, [{probability, 0.005}, {bytes, 150}, {capacity, 4000}, {size, 8000}]}
            ]},
    Merged = collapse_list_info([Info1, Info2, Info3, Info4]),
    Expect = [
            {<<"Test">>, [{bytes, 350}, {size, 7000}, {probability, 0.005}, {capacity, 6000}]},
            {<<"Foo">>, [{bytes, 350}, {size, 11000}, {probability, 0.005}, {capacity, 6000}]}
            ],
    ?assertEqual(Expect, Merged).

hash_slice_test() ->
    Idx = hash_slice(<<"foo">>, 5),
    Expect = riak_core_util:chash_std_keyfun({<<"foo">>, 5}),
    ?assertEqual(Expect, Idx).

-endif.
