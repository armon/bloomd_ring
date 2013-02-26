-module(br_repair).
-export([maybe_drop/1, maybe_create/1]).

% This method is invoked when a filter might be inconsistent
% and requires repair. If by some query it appears that there
% is a phantom slice, we can detect this and issue a drop.
maybe_drop(Filter) ->
    % TODO
    ok.


% This method is invoked when a filter might be inconsistent
% and requires repair. If by some query it appears that there
% is a slice missing, we can detect this and issue a create.
maybe_create(Filter) ->
    % TODO
    ok.

