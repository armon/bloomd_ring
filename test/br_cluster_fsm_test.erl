-module(br_cluster_fsm_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

main_test_() ->
    {foreach,
     spawn,
     fun setup/0,
     fun cleanup/1,
     [
     ]}.

setup() -> ok.
cleanup(_) -> ok.
