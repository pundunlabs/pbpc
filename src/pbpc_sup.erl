-module(pbpc_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
	 start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_link(pbpc_session_sup) ->
    supervisor:start_link({local, pbpc_session_sup},
			  ?MODULE, [pbpc_session_sup]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {one_for_one, 5, 10}, []} };
init([pbpc_session_sup]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{pbpc_session, {pbpc_session, start_link, []},
            temporary, 2000, worker, [pbpc_session]}]}}.

