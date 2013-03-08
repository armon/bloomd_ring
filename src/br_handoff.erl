% This module is used to do the heavy lifting for handoffs
-module(br_handoff).
-export([handoff/3, encode/2, decode/1, handle_receive/1]).
-include_lib("kernel/include/file.hrl").

% This constant defines the chunk size we use for transfering
% data. We want a chunk size that is large enough to be efficient
% for network and disk transfer, and small enough that progress
% can be made.
-define(CHUNK_SIZE, 4194304).


% This method is invoked to perform the hand off of data
% from the given index. We repeatedly call FoldFun/3 until
% all the data is handed off. The return value of handoff is
% the final return value of the FoldFun.
handoff(Idx, FoldFun, Accum) ->
    % Get the data dir
    {ok, DataDir} = application:get_env(bloomd_ring, bloomd_data_dir),

    % Search for local data contents
    {ok, Contents} = file:list_dir(DataDir),

    % Create a match pattern that matches the index only
    {ok, Re} = re:compile(iolist_to_binary(["^bloomd.", integer_to_list(Idx), ":"])),

    % Find all matching directories
    Matching = [C || C <- Contents, re:run(C, Re) =/= nomatch],

    % Fold over each directory
    lists:foldl(fun(Dir, Acc) ->
        FullDir = filename:join(DataDir, Dir),
        handoff_dir(FullDir, FoldFun, Acc)
    end, Accum, Matching).


% Encodes a key/value pair for handoff. This takes
% the key/value terms that are handed to the FoldFun
% in handoff, and prepares them to be transmitted as a
% binary.
-spec encode(term(), term()) -> binary().
encode(Key, Value) ->
    % Compress the value
    Compressed = zlib:zip(Value),

    % Convert the key to a binary
    Header = term_to_binary(Key),
    HeaderSize = size(Header),

    % Pack all the part together, length prefix the header.
    % This makes the assumption that a header is always smaller
    % than 16MB, which seems fairly sane.
    <<HeaderSize:24, Header/binary, Compressed/binary>>.


% Decodes the data that is encoded using encode,
% and prepares the result to be handed to handle_receive.
-spec decode(binary()) -> term().
decode(Data) ->
    % Get the header size
    <<HeaderSize:24, Rest/binary>> = Data,

    % Unpack the header and compressed body
    <<Header:HeaderSize/binary, Compressed/binary>> = Rest,

    % Unpack everything
    Key = binary_to_term(Header),
    Value = zlib:unzip(Compressed),
    {Key, Value}.


% Handles the data that is decoded using decode,
% and which was originally send via handoff invoking
% the FoldFun
-spec handle_receive(term()) -> ok.
handle_receive(Input) ->
    ok.

% This method is used to hand off the contents
% of a single folder.
handoff_dir(Dir, FoldFun, Accum) ->
    % Search for local contents
    {ok, Contents} = file:list_dir(Dir),
    lists:foldl(fun(File, Acc) ->
        handoff_file(Dir, File, FoldFun, Acc)
    end, Accum, lists:sort(Contents)).

% This method is used to hand off the contents
% of a single file.
handoff_file(Dir, File, FoldFun, Accum) ->
    case File of
        % Move the config file in a single read
        "config.ini" ->
            Path = filename:join(Dir, File),
            {ok, Bin} = file:read_file(Path),
            FoldFun({file, Path}, Bin, Accum);

        % Move data files piecewise
        "data." ++ _End ->
            % Get info about the file
            Path = filename:join(Dir, File),
            {ok, Info} = file:read_file_info(Path),

            % Determine the offsets we use to transfer
            Size = Info#file_info.size,
            Offsets = lists:seq(0, Size, ?CHUNK_SIZE),

            % Open the file
            {ok, IoDev} = file:open(Path, [read, binary]),

            % Fold over each offset
            NewAcc = lists:foldl(fun(Offset, Acc) ->
                % Read a single chunk
                {ok, Bin} = file:pread(IoDev, Offset, ?CHUNK_SIZE),

                % Send the partial
                FoldFun({partial, Path, Offset, Size}, Bin, Acc)
            end, Accum, Offsets),

            % Close the file handle and return
            ok = file:close(IoDev),
            NewAcc;

        % Unknown file, skip it
        _ ->
            lager:notice("Ignoring file ~p in ~p for handoff", [File, Dir]),
            Accum
    end.

