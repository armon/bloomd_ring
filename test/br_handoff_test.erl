-module(br_handoff_test).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").
-compile(export_all).

encode_decode_test() ->
    K = {test, a, key},
    V = <<"big old binary string">>,
    Enc = br_handoff:encode(K, V),
    Dec = br_handoff:decode(Enc),
    ?assertEqual({K, V}, Dec).

handoff_test() ->
    % Setup the folder
    ok = application:set_env(bloomd_ring, bloomd_data_dir, "/tmp"),

    % Mock the file list
    FolderList = ["bloomd.0:foo:1"],
    FileList = ["data.000.mmap", "config.ini", "tubez"],
    Info = #file_info{size=5000000},
    M = em:new(),
    em:strict(M, file, list_dir, ["/tmp"], {return, {ok, FolderList}}),
    em:strict(M, file, list_dir, ["/tmp/bloomd.0:foo:1"], {return, {ok, FileList}}),
    em:strict(M, file, read_file, ["/tmp/bloomd.0:foo:1/config.ini"], {return, {ok, configfile}}),
    em:strict(M, file, read_file_info, ["/tmp/bloomd.0:foo:1/data.000.mmap"], {return, {ok, Info}}),
    em:strict(M, file, open, ["/tmp/bloomd.0:foo:1/data.000.mmap",
                              [read, binary]], {return, {ok, filehandle}}),
    em:strict(M, file, pread, [filehandle, 0, 4194304], {return, {ok, data1}}),
    em:strict(M, file, pread, [filehandle, 4194304, 4194304], {return, {ok, data2}}),
    em:strict(M, file, close, [filehandle], {return, ok}),
    ok = em:replay(M),

    % Try to handoff
    FoldFun = fun(_, _, Acc) -> Acc+1 end,
    NewAcc = br_handoff:handoff(0, FoldFun, 0),
    ?assertEqual(3, NewAcc),
    em:verify(M).

