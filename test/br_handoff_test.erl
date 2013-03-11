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
    FoldFun = fun({file, "/tmp/bloomd.0:foo:1/config.ini"}, _, {FileAcc, PartAcc, FiltAcc}) ->
            {FileAcc+1 , PartAcc, FiltAcc};
        ({partial, "/tmp/bloomd.0:foo:1/data.00" ++ _More, _, _}, _, {FileAcc, PartAcc, FiltAcc}) ->
            {FileAcc, PartAcc+1, FiltAcc};
        ({filter, <<"0:foo:1">>}, _, {FileAcc, PartAcc, FiltAcc}) ->
            {FileAcc, PartAcc, FiltAcc+1}
    end,
    NewAcc = br_handoff:handoff(0, FoldFun, {0, 0, 0}),
    ?assertEqual({1, 2, 1}, NewAcc),
    em:verify(M).

handoff_receive_file_test() ->
    M = em:new(),
    em:strict(M, file, make_dir, ["."], {return, ok}),
    em:strict(M, file, write_file, [pathtofile, data], {return, ok}),
    ok = em:replay(M),

    ok = br_handoff:handle_receive({{file, pathtofile}, data}),
    em:verify(M).

handoff_receive_partial_test() ->
    M = em:new(),
    em:strict(M, file, make_dir, ["."], {return, ok}),
    em:strict(M, file, open, [pathtofile, [write, binary]], {return, {ok, iodev}}),
    em:strict(M, file, position, [iodev, 1024000], {return, {ok, 1024000}}),
    em:strict(M, file, truncate, [iodev], {return, ok}),
    em:strict(M, file, position, [iodev, 0], {return, {ok, 0}}),

    em:strict(M, file, pwrite, [iodev, 0, <<"data">>], {return, ok}),
    em:strict(M, file, pwrite, [iodev, 1024000-4, <<"more">>], {return, ok}),

    em:strict(M, file, close, [iodev], {return, ok}),
    ok = em:replay(M),

    ok = br_handoff:handle_receive({{partial, pathtofile, 0, 1024000}, <<"data">>}),
    ok = br_handoff:handle_receive({{partial, pathtofile, 1024000-4, 1024000}, <<"more">>}),
    em:verify(M).

handoff_receive_filter_test() ->
    % Setup the settings
    ok = application:set_env(bloomd_ring, bloomd_local_host, host),
    ok = application:set_env(bloomd_ring, bloomd_local_port, port),

    M = em:new(),
    em:strict(M, bloomd, new, [host, port, false], {return, conn}),
    em:strict(M, bloomd, create, [conn, <<"test">>, []], {return, done}),
    ok = em:replay(M),

    ok = br_handoff:handle_receive({{filter, <<"test">>}, <<"foo">>}),
    em:verify(M).

make_pathdir_test() ->
    M = em:new(),
    em:strict(M, file, make_dir, ["/tmp/bloomd"], {return, ok}),
    ok = em:replay(M),

    ok = br_handoff:make_pathdir("/tmp/bloomd/file.txt"),
    em:verify(M).

