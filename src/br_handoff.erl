% This module is used to do the heavy lifting for handoffs
-module(br_handoff).
-export([handoff/3]).

% This method is invoked to perform the hand off of data
% from the given index. We repeatedly call FoldFun/3 until
% all the data is handed off. The return value of handoff is
% the final return value of the FoldFun.
handoff(_Index, _FoldFun, _Accum) ->
    ok.

