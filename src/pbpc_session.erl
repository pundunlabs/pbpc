%%%===================================================================
%% @author Erdem Aksu
%% @copyright 2015 Pundun Labs AB
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
%% implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%% -------------------------------------------------------------------
%% @doc
%% Pundun Binary Protocol Client Session Handler
%% @end
%%%===================================================================

-module(pbpc_session).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1]).

-export([connect/4,
	 disconnect/1,
	 create_table/4,
	 delete_table/2,
	 open_table/2,
	 close_table/2,
	 table_info/2,
	 table_info/3,
	 read/3,
	 write/4,
	 update/4,
	 delete/3,
	 read_range/5,
	 read_range_n/4,
	 batch_write/4,
	 first/2,
	 last/2,
	 seek/3,
	 next/2,
	 prev/2,
	 add_index/3,
	 remove_index/3,
	 index_read/5]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
         terminate/2,
	 code_change/3,
	 handle_incomming_data/3]).

-include("pbpc.hrl").
-include("apollo_pb.hrl").
-include_lib("gb_log/include/gb_log.hrl").

-record(request, {transaction_id,
		  pdu,
		  from}).

-define(TID_THRESHOLD, 65535).
-define(REQ_TIMEOUT, 30000).

-type pbp_table_option() :: {type, 'LEVELDB' | 'MEMLEVELDB' | 'LEVELDBWRAPPED' | 'MEMLEVELDBWRAPPED' | 'LEVELDBTDA' | 'MEMLEVELDBTDA'} |
			    {data_model, 'KV' | 'ARRAY' | 'MAP'} |
			    {wrapper, #'Wrapper'{}} |
			    {mem_wrapper, #'Wrapper'{}} |
			    {comparator, 'DESCENDING' | 'ASCENDING'} |
			    {time_series, boolean()} |
			    {shards, integer()} |
			    {distributed, boolean()} |
			    {replication_factor, integer()} |
			    {hash_exclude, [string()]} |
			    {hashing_method, 'VIRTUALNODES' | 'CONSISTENT' | 'UNIFORM' | 'RENDEZVOUS'} |
			    {tda, #'Tda'{}}.
%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% Starts a server that manages a pbpc session.
%% @end
%%--------------------------------------------------------------------
-spec start_link(Args :: [{atom(), term()} | atom()]) ->
    {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

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
    ChildArgs = [{pid, self()},
		 {ip, IP}, {port, Port},
		 {username, Username},
		 {password, Password}],
    case supervisor:start_child(pbpc_session_sup, [ChildArgs]) of
	{ok, Pid} ->
	    {ok, Pid};
	{error, Reason} ->
	    {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Disconnect from pundun host by providing Session handler pid.
%% @end
%%--------------------------------------------------------------------
-spec disconnect(Session :: pid()) ->
    ok.
disconnect(Session) ->
    gen_server:cast(Session, disconnect).

%%--------------------------------------------------------------------
%% @doc
%% Create table on pundun nodes/cluster.
%% @end
%%--------------------------------------------------------------------
-spec create_table(Session :: pid(), TabName :: string(),
		   KeyDef :: [atom()], Options :: [table_option()])->
    ok | {error, Reason :: term()}.
create_table(Session, TabName, KeyDef, Options) ->
    P = #'CreateTable'{table_name = TabName,
		       keys = KeyDef,
		       table_options = make_set_of_table_option(Options)},
    transaction(Session, {create_table, P}).

%%--------------------------------------------------------------------
%% @doc
%% Delete table from pundun nodes/cluster.
%% @end
%%--------------------------------------------------------------------
-spec delete_table(Session :: pid(), TabName :: string())->
    ok | {error, Reason :: term()}.
delete_table(Session, TabName) ->
    P = #'DeleteTable'{table_name = TabName},
    transaction(Session, {delete_table, P}).

%%--------------------------------------------------------------------
%% @doc
%% Open table a pundun table.
%% @end
%%--------------------------------------------------------------------
-spec open_table(Session :: pid(), TabName :: string())->
    ok | {error, Reason :: term()}.
open_table(Session, TabName) ->
    P = #'OpenTable'{table_name = TabName},
    transaction(Session, {open_table, P}).

%%--------------------------------------------------------------------
%% @doc
%% Close a pundun table.
%% @end
%%--------------------------------------------------------------------
-spec close_table(Session :: pid(), TabName :: string())->
    ok | {error, Reason :: term()}.
close_table(Session, TabName) ->
    P = #'CloseTable'{table_name = TabName},
    transaction(Session, {close_table, P}).

%%--------------------------------------------------------------------
%% @doc
%% Get table information for all attributes.
%% @end
%%--------------------------------------------------------------------
-spec table_info(Session :: pid(),
		 TabName :: string()) ->
    [{atom(), term()}] | {error, Reason :: term()}.
table_info(Session, TabName) ->
    P = #'TableInfo'{table_name = TabName},
    transaction(Session, {table_info, P}).

%%--------------------------------------------------------------------
%% @doc
%% Get table information with desired attributes.
%% @end
%%--------------------------------------------------------------------
-spec table_info(Session :: pid(),
		 TabName :: string(),
		 Attributes :: [string()]) ->
    [{atom(), term()}] | {error, Reason :: term()}.
table_info(Session, TabName, Attributes) ->
    P = #'TableInfo'{table_name = TabName,
		     attributes = Attributes},
    transaction(Session, {table_info, P}).

%%--------------------------------------------------------------------
%% @doc
%% Read Key from the table TabName.
%% @end
%%--------------------------------------------------------------------
-spec read(Session :: pid(),
	   TabName :: string(),
	   Key :: key())->
    {ok, value()} | {error, Reason :: term()}.
read(Session, TabName, Key) ->
    P = #'Read'{table_name = TabName,
		key = make_seq_of_fields(Key)},
    transaction(Session, {read, P}).

%%--------------------------------------------------------------------
%% @doc
%% Write Key:Columns to the table TabName.
%% @end
%%--------------------------------------------------------------------
-spec write(Session :: pid(),
	    TabName :: string(),
	    Key :: key(),
	    Columns :: [column()])->
    ok | {error, Reason :: term()}.
write(Session, TabName, Key, Columns) ->
    P = #'Write'{table_name = TabName,
		 key = make_seq_of_fields(Key),
		 columns = make_seq_of_fields(Columns)},
    transaction(Session, {write, P}).

%%--------------------------------------------------------------------
%% @doc
%% Update Key according to Op on the table TabName.
%% field_name() :: string().
%% threshold() :: pos_integer().
%% setvalue() :: pos_integer().
%% update_instruction() :: increment |
%%                         {increment, threshold(), setvalue()} |
%%                         overwrite.
%% data() :: pos_integer() | term().
%% default() :: pos_integer() | term().
%% Op :: [{field_name(), instruction(), data()} |
%%        {field_name(), instruction(), data(), default()}].
%% @end
%%--------------------------------------------------------------------
-spec update(Session :: pid(),
	     TabName :: string(),
	     Key :: key(),
	     Op :: [update_op()])->
    {ok, value()} | {error, Reason :: term()}.
update(Session, TabName, Key, Op) ->
    P = #'Update'{table_name = TabName,
		  key = make_seq_of_fields(Key),
		  update_operation = make_update_operations(Op)},
    transaction(Session, {update, P}).

%%--------------------------------------------------------------------
%% @doc
%% Delete Key from the table TabName.
%% @end
%%--------------------------------------------------------------------
-spec delete(Session :: pid(),
	     TabName :: string(),
	     Key :: key())->
    ok | {error, Reason :: term()}.
delete(Session, TabName, Key) ->
    P = #'Delete'{table_name = TabName,
		  key = make_seq_of_fields(Key)},
    transaction(Session, {delete, P}).

%%--------------------------------------------------------------------
%% @doc
%% Read a Range of Keys from table with name Name and returns max
%% Chunk items from each local shard of the table
%% @end
%%--------------------------------------------------------------------
-spec read_range(Session :: pid(),
		 TabName :: string(),
		 StartKey :: key(),
		 EndKey :: key(),
		 Chunk :: pos_integer()) ->
    {ok, [kvp()], Cont::complete | key()} | {error, Reason :: term()}.
read_range(Session, TabName, StartKey, EndKey, Chunk) ->
    P = #'ReadRange'{table_name = TabName,
		     start_key = make_seq_of_fields(StartKey),
		     end_key = make_seq_of_fields(EndKey),
		     limit = Chunk},
    transaction(Session, {read_range, P}).

%%--------------------------------------------------------------------
%% @doc
%% Reads N nuber of Keys from table with name Name starting form
%% StartKey.
%% @end
%%--------------------------------------------------------------------
-spec read_range_n(Session :: pid(),
		   TabName :: string(),
		   StartKey :: key(),
		   N :: pos_integer()) ->
    {ok, [kvp()]} | {error, Reason :: term()}.
read_range_n(Session, TabName, StartKey, N) ->
    P = #'ReadRangeN'{table_name = TabName,
		      start_key = make_seq_of_fields(StartKey),
		      n = N},
    transaction(Session, {read_range_n, P}).

%%--------------------------------------------------------------------
%% @doc
%% Batch Write deletes and writes a batch of keys and values from/to
%% table in one operation.
%% @end
%%--------------------------------------------------------------------
-spec batch_write(Session :: pid(),
		  TabName :: string(),
		  DeleteKeys :: [key()],
		  WriteKvps :: [kvp()]) ->
    ok | {error, Reason :: term()}.
batch_write(Session, TabName, DeleteKeys, WriteKvps) ->
    P = #'BatchWrite'{table_name = TabName,
		      delete_keys = make_keys(DeleteKeys),
		      write_kvps = make_kvls(WriteKvps)},
    transaction(Session, {batch_write, P}).

%%--------------------------------------------------------------------
%% @doc
%% Get first key/value pair from the table TabName.
%% @end
%%--------------------------------------------------------------------
-spec first(Session :: pid(),
	    TabName :: string()) ->
    {ok, KVP :: kvp(), Ref :: pid()} |
    {error, Reason :: invalid | term()}.
first(Session, TabName) ->
    P = #'First'{table_name = TabName},
    transaction(Session, {first, P}).

%%--------------------------------------------------------------------
%% @doc
%% Get last key/value pair from the table TabName.
%% @end
%%--------------------------------------------------------------------
-spec last(Session :: pid(),
	   TabName :: string()) ->
    {ok, KVP :: kvp(), Ref :: pid()} |
    {error, Reason :: invalid | term()}.
last(Session, TabName) ->
    P = #'Last'{table_name = TabName},
    transaction(Session, {last, P}).

%%--------------------------------------------------------------------
%% @doc
%% Get the sought Key/Value from table that is specified by TabName.
%% @end
%%--------------------------------------------------------------------
-spec seek(Session :: pid(),
	   TabName :: string(),
	   Key :: key()) ->
    {ok, KVP :: kvp(), Ref :: pid()} |
    {error, Reason :: invalid | term()}.
seek(Session, TabName, Key) ->
    P = #'Seek'{table_name = TabName,
		key = make_seq_of_fields(Key)},
    transaction(Session, {seek, P}).

%%--------------------------------------------------------------------
%% @doc
%% Get the next Key/Value from table that is specified by iterator
%% reference Ref.
%% @end
%%--------------------------------------------------------------------
-spec next(Session :: pid(),
	   Ref :: pid()) ->
    {ok, KVP :: kvp()} |
    {error, Reason :: invalid | term()}.
next(Session, Ref) ->
    P = #'Next'{it = Ref},
    transaction(Session, {next, P}).

%%--------------------------------------------------------------------
%% @doc
%% Get the prevoius Key/Value from table that is specified by iterator
%% reference Ref.
%% @end
%%--------------------------------------------------------------------
-spec prev(Session :: pid(),
	   Ref :: pid()) ->
    {ok, KVP :: kvp()} |
    {error, Reason :: invalid | term()}.
prev(Session, Ref) ->
    P = #'Prev'{it = Ref},
    transaction(Session, {prev, P}).

%%--------------------------------------------------------------------
%% @doc
%% Add index on given columns for the table.
%% @end
%%--------------------------------------------------------------------
-spec add_index(Session :: pid(),
		TabName :: string(),
		IndexConfig :: [{string(), map()}]) ->
    ok | {error, Reason :: term()}.
add_index(Session, TabName, IndexConfig) ->
    P = #'AddIndex'{table_name = TabName,
		    config = make_index_config(IndexConfig)},
    transaction(Session, {add_index, P}).

%%--------------------------------------------------------------------
%% @doc
%% Remove index on given columns for the table.
%% @end
%%--------------------------------------------------------------------
-spec remove_index(Session :: pid(),
		   TabName :: string(),
		   Columns :: [string()]) ->
    ok | {error, Reason :: term()}.
remove_index(Session, TabName, Columns) ->
    P = #'RemoveIndex'{table_name = TabName,
		       columns = Columns},
    transaction(Session, {remove_index, P}).

%%--------------------------------------------------------------------
%% @doc
%% Index read on given tables column.
%% @end
%%--------------------------------------------------------------------
-spec index_read(Session :: pid(),
		 TabName :: string(),
		 Column :: string(),
		 Term :: string(),
		 Limit :: integer() | undefined) ->
    ok | {error, Reason :: term()}.
index_read(Session, TabName, Column, Term, Limit) ->
    P = #'IndexRead'{table_name = TabName,
		     column_name = Column,
		     term = Term,
		     limit = Limit},
    transaction(Session, {index_read, P}).

%%--------------------------------------------------------------------
%% @doc
%% Handle the received response messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_incomming_data(CorrId :: binary(),
			    Data :: list(),
			    TransactionRegister :: pid())->
    ok.
handle_incomming_data(CorrID, Data, TransactionRegister) ->
    case lookup_request(TransactionRegister, CorrID) of
	{ok, Client} ->
	    Client ! {response, Data},
	    true = ets:delete(TransactionRegister, CorrID),
	    ok;
	{error, not_found} ->
	    ?debug("No Pid found to reply..", []),
	    ok
    end.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

-spec init(Args :: [{atom(), term()}]) ->
    {ok, State :: map()} | {stop, Reason :: term()}.
init(Args) ->
    Pid = proplists:get_value(pid, Args),
    MonitorRef = erlang:monitor(process, Pid),
    IP = proplists:get_value(ip, Args),
    Port = proplists:get_value(port, Args),
    Username = proplists:get_value(username, Args),
    Password = proplists:get_value(password, Args),
    case connect_(IP, Port, Username, Password) of
	{ok, Socket} ->
	    ssl:setopts(Socket, [{packet, 4}]),
	    TR = ets:new(transaction_register, [set, public, {keypos, 1}]),
	    Counter = ets:new(counter, [set, public]),
	    ets:insert(Counter, {transaction_id, ?TID_THRESHOLD}),
	    {ok, #{socket => Socket,
		   transaction_register => TR,
		   counter => Counter,
		   timeout => ?REQ_TIMEOUT,
		   monitor_ref => MonitorRef}};
	{error, _Reason} ->
	    {stop, normal}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(),
		  From :: {pid(), Tag :: term()},
		  State :: #{}) ->
    {reply, Reply, State} |
    {reply, Reply, State, Timeout} |
    {noreply, State} |
    {noreply, State, Timeout} |
    {stop, Reason, Reply, State} |
    {stop, Reason, State}.
handle_call(get_state, _From, State) ->
    {reply, State, State};
handle_call(_Request, _From, State) ->
    ?notice("Unhandled gen_server Request: ~p, From ~p, State: ~p",
	    [_Request, _From, State]),
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Msg :: term(), State :: map()) ->
    {noreply, State :: map()} |
    {noreply, State :: map(), Timeout :: integer()} |
    {stop, Reason :: term(), State :: map()}.
handle_cast(disconnect, State = #{socket := Socket}) ->
    ?debug("Closing socket: ~p", [Socket]),
    ok = ssl:close(Socket),
    {stop, normal, State};
handle_cast(_Msg, State) ->
    ?debug("Unhandled cast message received: ~p", [_Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: term, State :: map()) ->
    {noreply, State :: map()} |
    {noreply, State :: map(), Timeout :: integer()} |
    {stop, Reason :: term(), State :: map()}.
handle_info({ssl_closed, Port}, State) ->
    ?debug("ssl_closed: ~p, stopping..", [Port]),
    {stop, ssl_closed, State};
handle_info({ssl, Socket, <<B1,B2, Data/binary>>},
	    State = #{transaction_register := TR}) ->
    ?debug("Received ssl data: ~p",[Data]),
    spawn(?MODULE, handle_incomming_data, [<<B1,B2>>, Data, TR]),
    ok = ssl:setopts(Socket, [{active, once}]),
    {noreply, State};
handle_info({'DOWN', Ref, process, _Pid, Reason},
	    State = #{monitor_ref := Ref}) ->
    ?debug("Stopping after socket owner process is 'DOWN', ~p", [Reason]),
    {stop, normal, State};
handle_info(_Info, State) ->
    ?debug("Unhandled info received: ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, #{monitor_ref := Ref}) ->
    erlang:demonitor(Ref),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
% ------------------------------------------------------------------
-spec transaction(Session :: pid(),
		  Procedure :: {atom(), term()}) -> term().
transaction(Session, Procedure) ->
    #{socket := Socket,
      transaction_register := TR,
      counter := Counter,
      timeout := TO} = gen_server:call(Session, get_state),
    {Tid, PDU} = get_pdu(Counter, Procedure),
    ?debug("Encoding PDU: ~p", [PDU]),
    Bin = apollo_pb:encode_msg(PDU),
    CorrId = encode_unsigned_16(Tid),
    case send(Socket, CorrId, Bin) of
	ok ->
	    true = ets:insert(TR, {CorrId, self()}),
	    receive
		{response, Data} ->
		    ?debug("Response received: ~p", [Data]),
		    decode(Data)
	    after
		TO ->
		    {error, timeout}
	    end;
	{error, Reason} ->
	    {error, Reason}
    end.

-spec send(Socket :: port(),
	   CorrId :: binary(),
	   BinData :: term()) ->
    ok | {error, Reason :: term()}.
send(Socket, CorrId, Bin) when is_binary(Bin) ->
    case ssl:send(Socket, [CorrId, Bin]) of
	ok ->
	    ok;
	{error, Reason} ->
	    {error, Reason}
    end;
send(_Socket, _, {error, Reason}) ->
    {error, Reason}.

-spec encode_unsigned_16(Int :: integer()) ->
    binary().
encode_unsigned_16(Int) ->
    case binary:encode_unsigned(Int, big) of
	<<B>> -> <<0,B>>;
	<<B1,B2>> -> <<B1, B2>>
    end.

-spec connect_(IP :: string(),
	      Port :: pos_integer(),
	      Username :: string(),
	      Password :: string()) ->
    {ok, Socket :: port()} | {error, Reason :: term()}.
connect_(IP, Port, Username, Password) ->
    case ssl:connect(IP, Port, [{active,false},
				{server_name_indication, disable}]) of
	{ok, Socket} ->
	    authenticate(Socket, Username, Password);
	{error, Reason} ->
	    {error, Reason}
    end.

-spec authenticate(Socket :: port(),
		   Username :: string(),
		   Password :: string()) ->
    {ok, Socket :: port()} | {error, Reason :: term()}.
authenticate(Socket, Username, Password) ->
    Map = maps:from_list([{socket, Socket},
			  {username, Username},
			  {password, Password}]),
    authenticate(init, Map).

-spec authenticate(Phase :: atom(), Map :: map()) ->
    {ok, Socket :: port()} | {error, Reason :: term()}.
authenticate(init, Map) ->
    Socket = maps:get(socket, Map),
    Username = maps:get(username, Map),
    ClientFirstMsgBare = scramerl:client_first_message_bare(Username),
    Gs2Header = scramerl:gs2_header(),
    ClientFirstMsg = Gs2Header ++ ClientFirstMsgBare,
    case ssl:send(Socket, ClientFirstMsg) of
	ok ->
	    ok = ssl:setopts(Socket, [{active, once}]),
	    NewMap = maps:put(client_first_msg_bare, ClientFirstMsgBare, Map),
	    authenticate(wait_server_first_message, NewMap);
	{error, Reason} ->
	    {error, Reason}
    end;
authenticate(wait_server_first_message, Map) ->
    receive
	{ssl, Socket, Data} ->
	    ScramData = scramerl_lib:parse(Data),
	    Salt = base64:decode_to_string(maps:get(salt, ScramData)),
	    IterCount = maps:get('iteration-count', ScramData),
	    Nonce = maps:get(nonce, ScramData),
	    Password = maps:get(password, Map),
	    Normalized = stringprep:prepare(Password),

	    ClientFirstMsgBare =  maps:get(client_first_msg_bare, Map),
	    CFMWoP = scramerl:client_final_message_without_proof(Nonce),
	    ServerFirstMsg = Data,
	    AuthMessage = ClientFirstMsgBare ++ "," ++
			  ServerFirstMsg ++ "," ++
			  CFMWoP,

	    SaltedPassword = scramerl_lib:hi(Normalized, Salt, IterCount),
	    ?debug("SaltedPassword: ~p~n",[SaltedPassword]),
	    ?debug("AuthMessage: ~p~n",[AuthMessage]),
	    ClientFinalMsg = scramerl:client_final_message(Nonce,
							   SaltedPassword,
							   AuthMessage),
	    case ssl:send(Socket, ClientFinalMsg) of
		ok ->
		    NewMap1 = maps:put(salted_password, SaltedPassword, Map),
		    NewMap2 = maps:put(auth_message, AuthMessage, NewMap1),
		    ok = ssl:setopts(Socket, [{active, once}]),
		    authenticate(wait_server_final_message, NewMap2);
		{error, Reason} ->
		    {error, Reason}
	    end
    after
	5000 ->
	    ?debug("timeout at wait_server_first_message~n", []),
	    {error, timeout}
    end;
authenticate(wait_server_final_message, Map) ->
    receive
	{ssl, Socket, Data} ->
	    ScramData = scramerl_lib:parse(Data),
	    Result =
		case maps:get(verifier, ScramData, undefined) of
		    undefined ->
			handle_server_error(ScramData);
		    Verifier ->
			verify_server_signature(Socket, Verifier, Map)
		end,
	    ok = ssl:setopts(Socket, [{active, once}]),
	    Result
    after
	5000 ->
	    {error, timeout}
    end.

-spec verify_server_signature(Socket :: port(),
			      Verifier :: string(),
			      Map :: map()) ->
    {ok, Socket :: port()} | {error, Reason :: term()}.
verify_server_signature(Socket, Verifier, Map) ->
    SaltedPassword = maps:get(salted_password, Map),
    AuthMessage = maps:get(auth_message, Map),
    ?debug("SaltedPassword: ~p~n", [SaltedPassword]),
    ?debug("AuthMessage: ~p~n", [AuthMessage]),
    ?debug("Verifier: ~p~n", [Verifier]),
    case scramerl:server_signature(SaltedPassword, AuthMessage) of
	Verifier ->
	    ok = ssl:setopts(Socket, [{mode, binary}]),
	    {ok, Socket};
	Else ->
	    ?debug("ServerSignature: ~p ~n",[Else]),
	    {error, server_verification}
    end.

-spec handle_server_error(ScramData :: map()) ->
    {error, Reason :: term()}.
handle_server_error(ScramData) ->
    case maps:get('server-error', ScramData, undefined) of
	undefined ->
	    {error, server_verification};
	Error ->
	    ?debug("Server Error: ~p~n",[Error]),
	    {error, Error}
    end.

-spec lookup_request(TR :: pid(), CorrID :: binary()) ->
    {ok, Req :: #request{}} | {error, not_found}.
lookup_request(TR, CorrId) ->
    case ets:lookup(TR, CorrId) of
	[{_, From}] ->
	    {ok, From};
	[] ->
	    {error, not_found}
    end.

-spec get_pdu(Counter :: integer(),
	      Procedure :: {atom(), term()}) ->
    {integer(), #'ApolloPdu'{}}.
get_pdu(Counter, Procedure) ->
    Tid = get_transaction_id(Counter),
    {Tid, #'ApolloPdu'{version = get_version(),
		       transaction_id = Tid,
		       procedure = Procedure}}.

-spec get_version() ->
    #'Version'{}.
get_version() ->
    #'Version'{major = 1, minor = 0}.

-spec get_transaction_id(Counter :: integer()) ->
    Tid :: integer().
get_transaction_id(Counter) ->
    ets:update_counter(Counter, transaction_id, {2, 1, ?TID_THRESHOLD, 0}).


-spec decode(Data :: binary) ->
    Response :: term().
decode(Data)->
    PDU = apollo_pb:decode_msg(Data, 'ApolloPdu'),
    ?debug("Response decoded: ~p", [PDU]),
    get_return_value(PDU).

-spec get_return_value(PDU :: #'ApolloPdu'{}) ->
    Response :: term().
get_return_value(#'ApolloPdu'{procedure =
		    {response, #'Response'{result = {ok, "ok"}}}}) ->
    ok;
get_return_value(#'ApolloPdu'{procedure =
		    {response, #'Response'{result = {columns, Columns}}}}) ->
    {ok, strip_fields(Columns)};
get_return_value(#'ApolloPdu'{procedure =
		    {response, #'Response'{result =
			{key_columns_pair,
			    #'KeyColumnsPair'{key = Key,
					      columns = Columns}}}}}) ->
    {ok, {strip_fields(Key), strip_fields(Columns)}};
get_return_value(#'ApolloPdu'{procedure =
		    {response, #'Response'{result =
			{key_columns_list,
			    #'KeyColumnsList'{list = List,
					      continuation = Cont}}}}}) ->
    Kcl = [begin
	    {strip_fields(K), strip_fields(C)}
	   end || #'KeyColumnsPair'{key = K, columns = C} <- List],
    case Cont of
	undefined ->
	    {ok, Kcl};
	#'Continuation'{complete = true} ->
	    {ok, Kcl, complete};
	#'Continuation'{complete = false, key = Key} ->
	    {ok, Kcl, strip_fields(Key)}
    end;
get_return_value(#'ApolloPdu'{procedure =
		    {response, #'Response'{result =
			{proplist, Proplist}}}}) ->
    Result = strip_fields(Proplist),
    {ok, Result};
get_return_value(#'ApolloPdu'{procedure =
		    {response, #'Response'{result =
			{kcp_it, #'KcpIt'{key_columns_pair =
					    #'KeyColumnsPair'{key = K,
							      columns = V},
					 it = It}}}}}) ->
    {ok, {strip_fields(K), strip_fields(V)}, It};
get_return_value(#'ApolloPdu'{procedure =
		    {response, #'Response'{result =
			{postings, #'Postings'{list = Postings}}}}}) ->
    {ok, [#{key => strip_fields(K),
	    timestamp => Ts,
	    frequency => Freq,
	    position => Pos}
	    || #'Posting'{key = K,
			  timestamp = Ts,
			  frequency = Freq,
			  position = Pos} <- Postings]};
get_return_value(#'ApolloPdu'{procedure =
		    {error, #'Error'{cause = Cause}}}) ->
    {error, Cause}.

-spec make_set_of_table_option(Options :: [table_option()]) ->
    [#'TableOption'{}].
make_set_of_table_option(Options) ->
    List = make_set_of_table_option(Options, []),
    [#'TableOption'{opt = O} || O <- List].

-spec make_set_of_table_option(Options :: [table_option()],
			       Acc :: [pbp_table_option()]) ->
    [pbp_table_option()].
make_set_of_table_option([], Acc) ->
    lists:reverse(Acc);
make_set_of_table_option([{type, T}|Rest], Acc) ->
    make_set_of_table_option(Rest, [translate_options({type, T}) | Acc]);
make_set_of_table_option([{data_model, DT}|Rest], Acc) ->
    make_set_of_table_option(Rest, [translate_options({data_model, DT}) | Acc]);
make_set_of_table_option([{wrapper, Wrp} | Rest], Acc) ->
    Wrapper = setelement(1, Wrp, 'Wrapper'),
    make_set_of_table_option(Rest, [{wrapper, Wrapper} | Acc]);
make_set_of_table_option([{mem_wrapper, Wrp}|Rest], Acc) ->
    Wrapper = setelement(1, Wrp, 'Wrapper'),
    make_set_of_table_option(Rest, [{mem_wrapper, Wrapper} | Acc]);
make_set_of_table_option([{tda, #tda{num_of_buckets = NB,
				     time_margin = TM,
				     ts_field = TF,
				     precision = P}}|Rest], Acc) ->
    Tda = #'Tda'{num_of_buckets = NB,
		 time_margin = TM,
		 ts_field = TF,
		 precision = translate_precision(P)},
    make_set_of_table_option(Rest, [{tda, Tda} | Acc]);
make_set_of_table_option([{comparator, C}|Rest], Acc) ->
    make_set_of_table_option(Rest, [translate_options({comparator, C}) | Acc]);
make_set_of_table_option([{time_series, T}|Rest], Acc) ->
    make_set_of_table_option(Rest, [{time_series, T} | Acc]);
make_set_of_table_option([{num_of_shards, S}|Rest], Acc) ->
    make_set_of_table_option(Rest, [{num_of_shards, S} | Acc]);
make_set_of_table_option([{distributed, D}|Rest], Acc) ->
    make_set_of_table_option(Rest, [{distributed, D} | Acc]);
make_set_of_table_option([{replication_factor, RF}|Rest], Acc) ->
    make_set_of_table_option(Rest, [{replication_factor, RF} | Acc]);
make_set_of_table_option([{hash_exclude, HE}|Rest], Acc) ->
    make_set_of_table_option(Rest, [{hash_exclude, HE} | Acc]);
make_set_of_table_option([{hashing_method, HE}|Rest], Acc) ->
    make_set_of_table_option(Rest, [translate_options({hashing_method, HE}) | Acc]);
make_set_of_table_option([{ttl, TTL}|Rest], Acc) when is_integer(TTL) ->
    make_set_of_table_option(Rest, [{ttl, TTL} | Acc]);
make_set_of_table_option([_Else|Rest], Acc) ->
    ?debug("Unknown table option: ~p", [_Else]),
    make_set_of_table_option(Rest, Acc).

-spec make_seq_of_fields(Key :: [{string(), term()}]) ->
    [#'Field'{}].
make_seq_of_fields(Key) when is_list(Key)->
    [#'Field'{name = Name, value = make_value(Value)}
	|| {Name, Value} <- Key];
make_seq_of_fields(Else)->
    ?debug("Invalid key: ~p",[Else]),
    Else.

-spec make_value(V :: term()) ->
    {bool, Bool :: true | false} |
    {int, Int :: integer()} |
    {binary, Bin :: binary()} |
    {null, Null :: undefined} |
    {double, Double :: binary()} |
    {binary, Str :: binary()} |
    {string, Str :: [integer()]}.
make_value(V) when is_binary(V) ->
    {binary, V};
make_value(V) when is_integer(V) ->
    {int, V};
make_value(V) when is_float(V) ->
    {double, V};
make_value(true) ->
    {boolean, true};
make_value(false) ->
    {boolean, false};
make_value(V) when is_list(V) ->
    case is_list_of_printables(V) of
	{true, L} ->
	    {string, L};
	false ->
	    {binary, list_to_binary(V)}
    end;
make_value(undefined) ->
    {null, 'NULL'};
make_value(A) when is_atom(A) ->
    {string, atom_to_list(A)};
make_value(T) when is_tuple(T) ->
    {binary, term_to_binary(T)}.

is_list_of_printables(L) ->
    case io_lib:printable_unicode_list(L) of
        true -> {true, L};
	false ->
	    case io_lib:printable_unicode_list(lists:flatten(L)) of
		true ->
		    {true, lists:flatten([E++" "||E <- L])};
		false -> false
	    end
    end.

-spec make_keys(Keys :: [[{string(), term()}]]) ->
    [[#'Field'{}]].
make_keys(Keys) when is_list(Keys) ->
    [make_seq_of_fields(K) || K <- Keys];
make_keys(Else) ->
    ?debug("Invalid key list: ~p",[Else]),
    Else.

-spec make_kvls(Kvls :: [ {[{string(), term()}], [{string(), term()}]} ]) ->
    [#'KeyColumnsPair'{}].
make_kvls(Kvls) when is_list(Kvls)->
    [#'KeyColumnsPair'{key = make_seq_of_fields(Key),
		       columns = make_seq_of_fields(Columns)}
	|| {Key, Columns} <- Kvls];
make_kvls(Else) ->
    ?debug("Invalid key/columns tuple list: ~p",[Else]),
    Else.

-spec strip_fields(Fields :: [#'Field'{}] | #'Fields'{}) ->
    [{string(), term()}].
strip_fields(#'Fields'{fields = Fields}) ->
    strip_fields(Fields, []);
strip_fields(Fields) ->
    strip_fields(Fields, []).

-spec strip_fields(Fields :: [#'Field'{}],
		   Acc :: [{string(), term()}]) ->
    [{string(), term()}].
strip_fields([#'Field'{name = N, value = {boolean, B}} | Rest], Acc) ->
    Bool = case B of
	    0 -> false;
	    1 -> true;
	    B -> B
	   end,
    strip_fields(Rest, [{N, Bool} | Acc]);
strip_fields([#'Field'{name = N, value = {null, _}} | Rest], Acc) ->
    strip_fields(Rest, [{N, undefined} | Acc]);
strip_fields([#'Field'{name = N, value = {_, V}} | Rest], Acc) ->
    strip_fields(Rest, [{N, V} | Acc]);
strip_fields([], Acc) ->
    lists:reverse(Acc).

-spec translate_options(Option :: table_option()) ->
    PBP_Option :: pbp_table_option().
translate_options({type, rocksdb}) ->
    {type, 'ROCKSDB'};
translate_options({type, leveldb}) ->
    {type, 'LEVELDB'};
translate_options({type, mem_leveldb}) ->
    {type, 'MEMLEVELDB'};
translate_options({type, leveldb_wrapped}) ->
    {type, 'LEVELDBWRAPPED'};
translate_options({type, mem_leveldb_wrapped}) ->
    {type, 'MEMLEVELDBWRAPPED'};
translate_options({type, leveldb_tda}) ->
    {type, 'LEVELDBTDA'};
translate_options({type, mem_leveldb_tda}) ->
    {type, 'MEMLEVELDBTDA'};
translate_options({data_model, kv}) ->
    {data_model, 'KV'};
translate_options({data_model, array}) ->
    {data_model, 'ARRAY'};
translate_options({data_model, map}) ->
    {data_model, 'MAP'};
translate_options({comparator, descending}) ->
    {comparator, 'DESCENDING'};
translate_options({comparator, ascending}) ->
    {comparator, 'ASCENDING'};
translate_options({hashing_method, virtual_nodes}) ->
    {hashing_method, 'VIRTUALNODES'};
translate_options({hashing_method, consistent}) ->
    {hashing_method, 'CONSISTENT'};
translate_options({hashing_method, uniform}) ->
    {hashing_method, 'UNIFORM'};
translate_options({hashing_method, rendezvous}) ->
    {hashing_method, 'RENDEZVOUS'};
translate_options(Option) ->
    Option.

-spec translate_precision(P :: time_unit()) ->
    'SECOND' | 'MILLISECOND' | 'MICROSECOND' | 'NANOSECOND'.
translate_precision(second)-> 'SECOND';
translate_precision(millisecond)-> 'MILLISECOND';
translate_precision(microsecond)-> 'MICROSECOND';
translate_precision(nanosecond)-> 'NANOSECOND';
translate_precision(P)-> P.

-spec make_update_operations(Op :: update_op()) ->
    [#'UpdateOperation'{}].
make_update_operations(Op) ->
    make_update_operations(Op, []).

-spec make_update_operations(Op :: update_op(),
			     Acc :: [#'UpdateOperation'{}]) ->
    [#'UpdateOperation'{}].
make_update_operations([{F, Inst, Data} | Rest], Acc) ->
    UpOp = #'UpdateOperation'{field = F,
			      update_instruction = make_update_instruction(Inst),
			      value = #'Value'{value=make_value(Data)}},
    make_update_operations(Rest, [UpOp | Acc]);
make_update_operations([{F, Inst, Data, Default} | Rest], Acc) ->
    UpOp = #'UpdateOperation'{field = F,
			      update_instruction = make_update_instruction(Inst),
			      value = #'Value'{value=make_value(Data)},
			      default_value = #'Value'{value=make_value(Default)}},
    make_update_operations(Rest, [UpOp | Acc]);
make_update_operations([], Acc) ->
    lists:reverse(Acc).

-spec make_update_instruction(Inst :: update_instruction()) ->
    #'UpdateInstruction'{}.
make_update_instruction(increment) ->
    #'UpdateInstruction'{instruction = 'INCREMENT',
			 threshold = <<>>,
			 set_value = <<>>};
make_update_instruction({increment, T, S}) ->
    #'UpdateInstruction'{instruction = 'INCREMENT',
			 threshold = binary:encode_unsigned(T, big),
			 set_value = binary:encode_unsigned(S, big)};
make_update_instruction(overwrite) ->
    #'UpdateInstruction'{instruction = 'OVERWRITE',
			 threshold = <<>>,
			 set_value = <<>>}.

-spec make_index_config(IndexConfig :: [{Column :: string(), Opts :: map()}]) ->
    [#'IndexConfig'{}].
make_index_config(IndexConfig) ->
    make_index_config(IndexConfig, []).

make_index_config([{Column, Config} | Rest], Acc) ->
    IndexConfig = #'IndexConfig'{column = Column,
				 options = make_index_options(Config)},
    make_index_config(Rest, [IndexConfig | Acc]);
make_index_config([Column | Rest], Acc) when is_list(Column)->
    IndexConfig = #'IndexConfig'{column = Column},
    make_index_config(Rest, [IndexConfig | Acc]);
make_index_config([], Acc) ->
    lists:reverse(Acc).

make_index_options(Config) when is_map(Config) ->
   #'IndexOptions'{char_filter = make_char_filter(Config),
		   tokenizer = make_tokenizer(Config),
		   token_filter = make_token_filter(Config)};
make_index_options(_) ->
    undefined.

make_char_filter(#{char_filter := nfc}) ->
    'NFC';
make_char_filter(#{char_filter := nfd}) ->
    'NFD';
make_char_filter(#{char_filter := nfkc}) ->
    'NFKC';
make_char_filter(#{char_filter := nfkd}) ->
    'NFKD';
make_char_filter(_) ->
    undefined.

make_tokenizer(#{tokenizer := unicode_word_boundaries}) ->
    'UNICODE_WORD_BOUNDARIES';
make_tokenizer(_) ->
    undefined.

make_token_filter(#{token_filter := TokenFilter} ) ->
    #'TokenFilter'{transform = make_token_transform(TokenFilter),
		   add = make_token_add_filter(TokenFilter),
		   delete = make_token_delete_filter(TokenFilter),
		   stats = make_token_stats(TokenFilter)};
make_token_filter(_) ->
    undefined.

make_token_transform(#{transform := lowercase}) ->
    'LOWERCASE';
make_token_transform(#{transform := uppercase}) ->
    'UPPERCASE';
make_token_transform(#{transform := casefold}) ->
    'CASEFOLD';
make_token_transform(_) ->
    undefined.

make_token_add_filter(#{add := L}) when is_list(L) ->
    L;
make_token_add_filter(_)->
    [].

make_token_delete_filter(#{delete := Stopwords}) ->
    [handle_stopword(W) || W <- Stopwords];
make_token_delete_filter(_) ->
    [].

handle_stopword(english_stopwords) ->
    "$english_stopwords";
handle_stopword(lucene_stopwords) ->
    "$lucene_stopwords";
handle_stopword(wikipages_stopwords) ->
    "$wikipages_stopwords";
handle_stopword(W) ->
    W.

make_token_stats(#{stats := unique}) ->
    'UNIQUE';
make_token_stats(#{stats := freqs}) ->
    'FREQUENCY';
make_token_stats(#{stats := position}) ->
    'POSITION';
make_token_stats(_) ->
    'NOSTATS'.
