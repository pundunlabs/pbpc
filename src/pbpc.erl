-module(pbpc).

-export([connect/4]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Connect to a pundun host at IP:Port with given Username and
%% Password.
%% @end
%%--------------------------------------------------------------------
-spec connect(IP :: string(),
	      Port :: pos_integer(),
	      Username :: string(),
	      Password :: string()) ->
    {ok, Session :: pid()} | {error, Reason :: term()}.
connect(IP, Port, Username, Password) ->
     ChildArgs = [{ip, IP}, {port, Port},
		  {username, Username},
		  {password, Password}],
    case supervisor:start_child(pbpc_session_sup, [ChildArgs]) of
	{ok, Pid} ->
	    {ok, Pid};
	{error, Reason} ->
	    {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%% End of Module.
