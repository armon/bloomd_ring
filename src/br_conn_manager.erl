-module(br_conn_manager).
-export([start_handler/1]).

% Starts a new handler process for the given socket
% Return {ok, pid()}.
start_handler(Socket) ->
    % Create the spec
    HandlerSpec = {Socket,
                {br_ascii_handler, start_link, [Socket]},
                temporary, 5000, worker, dynamic},

    % Start the child
    supervisor:start_child(br_conn_manager_sup, HandlerSpec).

