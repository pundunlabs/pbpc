-module(pbpc_app).

-behaviour(application).

%% Application callbacks
-export([start/0, start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================
start() ->
    application:start(pbpc).

start(_StartType, _StartArgs) ->
    pbpc_sup:start_link(),
    pbpc_sup:start_link(pbpc_session_sup).

stop(_State) ->
    ok.
