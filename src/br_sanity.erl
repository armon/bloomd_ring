% This module helps to do sanity checking of the configuation
% of bloomd, and bloomd ring to ensure that operations will
% work as expected.
-module(br_sanity).
-export([verify/0, check_bloomd/0, check_paths/0]).

% We attempt to make a file with this name to test permissions
-define(TEST_FILENAME, "perm_test_bloomd_ring").

% Returns either true or false if the environment is
% sane operating environment
verify() ->
    % Check the local bloomd and the local paths
    check_bloomd() and check_paths().

% Checks that bloomd is operational and we can connect
check_bloomd() ->
    {ok, LocalHost} = application:get_env(bloomd_ring, bloomd_local_host),
    {ok, LocalPort} = application:get_env(bloomd_ring, bloomd_local_port),

    % Try to connect and do a list
    Res = try
        Conn = bloomd:new(LocalHost, LocalPort, false),
        ListRes = bloomd:list(Conn),
        close_conn(Conn),
        case ListRes of
            {error, Err} ->
                lager:error("Failed to list from bloomd! Got {error, ~p}", [Err]),
                false;
            _ -> true
        end
    catch
        Cls:Error ->
            lager:error("Failed to connect to bloomd with ~p:~p", [Cls, Error]),
            false
    end,

    % Return the final result
    Res.

% Attempts to close a socket without raising an exception
close_conn(Conn) ->
    try bloomd:close(Conn)
    catch
        _:_ -> ok
    end.

% Checks the local paths and permissions are all ok
check_paths() ->
    % Get the data dir
    {ok, DataDir} = application:get_env(bloomd_ring, bloomd_data_dir),

    % Check if this is a dir
    case filelib:is_dir(DataDir) of
        false ->
            lager:error("Configured data dir ~p is not a directory!", [DataDir]),
            false;

        true ->
            % Check if we can list the directory
            case file:list_dir(DataDir) of
                {error, Err} ->
                    lager:error("Can't access data dir ~p. Err: ~p", [DataDir, Err]),
                    false;

                {ok, _} ->
                    % Ensure we can read/write to the path
                    TestPath = filename:join(DataDir, ?TEST_FILENAME),
                    case file:open(TestPath, [read, write]) of
                        {error, Err} ->
                            lager:error("Permissions test failed for data dir ~p! Error: ~p", [DataDir, Err]),
                            false;

                        {ok, IoDev} ->
                            file:close(IoDev),
                            file:delete(TestPath),
                            true
                    end
            end
    end.

