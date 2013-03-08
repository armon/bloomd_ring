-module(br_handoff_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

encode_decode_test() ->
    K = {test, a, key},
    V = <<"big old binary string">>,
    Enc = br_handoff:encode(K, V),
    Dec = br_handoff:decode(Enc),
    ?assertEqual({K, V}, Dec).

