% This module is used to do the heavy lifting for handoffs
-module(br_handoff).
-export([handoff/3, encode/2, decode/1, handle_receive/1]).

% This method is invoked to perform the hand off of data
% from the given index. We repeatedly call FoldFun/3 until
% all the data is handed off. The return value of handoff is
% the final return value of the FoldFun.
handoff(_Index, _FoldFun, _Accum) ->
    ok.


% Encodes a key/value pair for handoff. This takes
% the key/value terms that are handed to the FoldFun
% in handoff, and prepares them to be transmitted as a
% binary.
-spec encode(term(), term()) -> binary().
encode(Key, Value) ->
    ok.

% Decodes the data that is encoded using encode,
% and prepares the result to be handed to handle_receive.
-spec decode(binary()) -> term().
decode(Data) ->
    ok.

% Handles the data that is decoded using decode,
% and which was originally send via handoff invoking
% the FoldFun
-spec handle_receive(term()) -> ok.
handle_receive(Input) ->
    ok.

