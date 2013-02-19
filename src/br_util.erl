-module(br_util).
-export([ceiling/1,
         num_partitions/0,
         keyslice/1,
         keyslice/2]).

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
keyslice(Key, Partitions) ->
    % Get a SHA1 digest
    <<Digest:160/integer>> = crypto:sha(term_to_binary(Key)),

    % Mod by the ring size
    Digest rem Partitions.

