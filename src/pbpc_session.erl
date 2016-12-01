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
	 prev/2]).

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

-record(state, {socket,
		transaction_register,
		counter}).

-record(request, {transaction_id,
		  pdu,
		  from}).

-define(TID_THRESHOLD, 65535).
-define(REQ_TIMEOUT, 30000).

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
    ChildArgs = [{ip, IP}, {port, Port},
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
    Data = gen_server:call(Session, {create_table, TabName, KeyDef, Options}, ?REQ_TIMEOUT),
    decode(Data).

%%--------------------------------------------------------------------
%% @doc
%% Delete table from pundun nodes/cluster.
%% @end
%%--------------------------------------------------------------------
-spec delete_table(Session :: pid(), TabName :: string())->
    ok | {error, Reason :: term()}.
delete_table(Session, TabName) ->
    Data = gen_server:call(Session, {delete_table, TabName}, ?REQ_TIMEOUT),
    decode(Data).

%%--------------------------------------------------------------------
%% @doc
%% Open table a pundun table.
%% @end
%%--------------------------------------------------------------------
-spec open_table(Session :: pid(), TabName :: string())->
    ok | {error, Reason :: term()}.
open_table(Session, TabName) ->
    Data = gen_server:call(Session, {open_table, TabName}, ?REQ_TIMEOUT),
    decode(Data).

%%--------------------------------------------------------------------
%% @doc
%% Close a pundun table.
%% @end
%%--------------------------------------------------------------------
-spec close_table(Session :: pid(), TabName :: string())->
    ok | {error, Reason :: term()}.
close_table(Session, TabName) ->
    Data = gen_server:call(Session, {close_table, TabName}, ?REQ_TIMEOUT),
    decode(Data).

%%--------------------------------------------------------------------
%% @doc
%% Get table information for all attributes.
%% @end
%%--------------------------------------------------------------------
-spec table_info(Session :: pid(),
		 TabName :: string()) ->
    [{atom(), term()}] | {error, Reason :: term()}.
table_info(Session, TabName) ->
    Data = gen_server:call(Session, {table_info, TabName}, ?REQ_TIMEOUT),
    decode(Data).

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
    Data = gen_server:call(Session, {table_info, TabName, Attributes}, ?REQ_TIMEOUT),
    decode(Data).

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
    Data = gen_server:call(Session, {read, TabName, Key}, ?REQ_TIMEOUT),
    decode(Data).

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
    Data = gen_server:call(Session, {write, TabName, Key, Columns}, ?REQ_TIMEOUT),
    decode(Data).

%%--------------------------------------------------------------------
%% @doc
%% Update Key according to Op on the table TabName.
%% field_name() :: string().
%% treshold() :: pos_integer().
%% setvalue() :: pos_integer().
%% update_instruction() :: increment |
%%                         {increment, treshold(), setvalue()} |
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
    Data = gen_server:call(Session, {update, TabName, Key, Op}, ?REQ_TIMEOUT),
    decode(Data).

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
    Data = gen_server:call(Session, {delete, TabName, Key}, ?REQ_TIMEOUT),
    decode(Data).

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
    Data = gen_server:call(Session, {read_range, TabName, StartKey, EndKey, Chunk}, ?REQ_TIMEOUT),
    decode(Data).

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
    Data = gen_server:call(Session, {read_range_n, TabName, StartKey, N}, ?REQ_TIMEOUT),
    decode(Data).

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
    Data = gen_server:call(Session, {batch_write, TabName, DeleteKeys, WriteKvps}, ?REQ_TIMEOUT),
    decode(Data).

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
    Data = gen_server:call(Session, {first, TabName}, ?REQ_TIMEOUT),
    decode(Data).

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
    Data = gen_server:call(Session, {last, TabName}, ?REQ_TIMEOUT),
    decode(Data).

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
    Data = gen_server:call(Session, {seek, TabName, Key}, ?REQ_TIMEOUT),
    decode(Data).

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
    Data = gen_server:call(Session, {next, Ref}, ?REQ_TIMEOUT),
    decode(Data).

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
    Data = gen_server:call(Session, {prev, Ref}, ?REQ_TIMEOUT),
    decode(Data).

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
	    gen_server:reply(Client, Data),
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
    {ok, State :: #state{}} | {stop, Reason :: term()}.
init(Args) ->
    IP = proplists:get_value(ip, Args),
    Port = proplists:get_value(port, Args),
    Username = proplists:get_value(username, Args),
    Password = proplists:get_value(password, Args),
    case connect_(IP, Port, Username, Password) of
	{ok, Socket} ->
	    ssl:setopts(Socket, [{packet, 4}]),
	    TR = ets:new(transaction_register, [set, public, {keypos, 1}]),
	    Counter = ets:new(counter, [set, protected]),
	    ets:insert(Counter, {transaction_id, ?TID_THRESHOLD}),
	    {ok, #state{socket = Socket,
			transaction_register = TR,
			counter = Counter}};
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
		  State :: #state{}) ->
    {reply, Reply, State} |
    {reply, Reply, State, Timeout} |
    {noreply, State} |
    {noreply, State, Timeout} |
    {stop, Reason, Reply, State} |
    {stop, Reason, State}.
handle_call({create_table, TableName, KeyDef, Options}, From, State) ->
    P = #'CreateTable'{table_name = TableName,
		       keys = KeyDef,
		       table_options = make_set_of_table_option(Options)},
    send_request({create_table, P}, From, State);
handle_call({delete_table, TableName}, From, State) ->
    P = #'DeleteTable'{table_name = TableName},
    send_request({delete_table, P}, From, State);
handle_call({open_table, TableName}, From, State) ->
    P = #'OpenTable'{table_name = TableName},
    send_request({open_table, P}, From, State);
handle_call({close_table, TableName}, From, State) ->
    P = #'CloseTable'{table_name = TableName},
    send_request({close_table, P}, From, State);
handle_call({table_info, TableName}, From, State) ->
    P = #'TableInfo'{table_name = TableName},
    send_request({table_info, P}, From, State);
handle_call({table_info, TableName, Attributes}, From, State) ->
    P = #'TableInfo'{table_name = TableName,
		     attributes = Attributes},
    send_request({table_info, P}, From, State);
handle_call({read, TableName, Key}, From, State) ->
    P = #'Read'{table_name = TableName,
		key = make_seq_of_fields(Key)},
    send_request({read, P}, From, State);
handle_call({write, TableName, Key, Columns}, From, State) ->
    P = #'Write'{table_name = TableName,
		 key = make_seq_of_fields(Key),
		 columns = make_seq_of_fields(Columns)},
    send_request({write, P}, From, State);
handle_call({update, TableName, Key, Op}, From, State) ->
    P = #'Update'{table_name = TableName,
		  key = make_seq_of_fields(Key),
		  update_operation = make_update_operations(Op)},
    send_request({update, P}, From, State);
handle_call({delete, TableName, Key}, From, State) ->
    P = #'Delete'{table_name = TableName,
		  key = make_seq_of_fields(Key)},
    send_request({delete, P}, From, State);
handle_call({read_range, TableName, StartKey, EndKey, Chunk}, From, State) ->
    P = #'ReadRange'{table_name = TableName,
		     start_key = make_seq_of_fields(StartKey),
		     end_key = make_seq_of_fields(EndKey),
		     limit = Chunk},
    send_request({read_range, P}, From, State);
handle_call({read_range_n, TableName, StartKey, N}, From, State) ->
    P = #'ReadRangeN'{table_name = TableName,
		      start_key = make_seq_of_fields(StartKey),
		      n = N},
    send_request({read_range_n, P}, From, State);
handle_call({batch_write, TableName, DeleteKeys, WriteKvls}, From, State) ->
    P = #'BatchWrite'{table_name = TableName,
		      delete_keys = make_keys(DeleteKeys),
		      write_kvps = make_kvls(WriteKvls)},
    send_request({batch_write, P}, From, State);
handle_call({first, TableName}, From, State) ->
    P = #'First'{table_name = TableName},
    send_request({first, P}, From, State);
handle_call({last, TableName}, From, State) ->
    P = #'Last'{table_name = TableName},
    send_request({last, P}, From, State);
handle_call({seek, TableName, Key}, From, State) ->
    P = #'Seek'{table_name = TableName,
		key = make_seq_of_fields(Key)},
    send_request({seek, P}, From, State);
handle_call({next, It}, From, State) ->
    P = #'Next'{it = It},
    send_request({next, P}, From, State);
handle_call({prev, It}, From, State) ->
    P = #'Prev'{it = It},
    send_request({prev, P}, From, State);
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
-spec handle_cast(Msg :: term(), State :: #state{}) ->
    {noreply, State :: #state{}} |
    {noreply, State :: #state{}, Timeout :: integer()} |
    {stop, Reason :: term(), State :: #state{}}.
handle_cast(disconnect, State = #state{socket = Socket}) ->
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
-spec handle_info(Info :: term, State :: #state{}) ->
    {noreply, State :: #state{}} |
    {noreply, State :: #state{}, Timeout :: integer()} |
    {stop, Reason :: term(), State :: #state{}}.
handle_info({ssl_closed, Port}, State) ->
    ?debug("ssl_closed: ~p, stopping..", [Port]),
    {stop, ssl_closed, State};
handle_info({ssl, Socket, <<B1,B2, Data/binary>>}, State = #state{transaction_register = TR}) ->
    ?debug("Received ssl data: ~p",[Data]),
    spawn_link(?MODULE, handle_incomming_data, [<<B1,B2>>, Data, TR]),
    ok = ssl:setopts(Socket, [{active, once}]),
    {noreply, State};
handle_info(_Info, State) ->
    ?debug("Unhandled info received: ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
% ------------------------------------------------------------------
-spec send_request(Procedure :: {atom(), term()},
		   From :: {pid(), Tag :: term()},
		   State :: #state{}) ->
    {noreply, State :: #state{}} |
    {reply, {error, Reason :: term()}, State :: #state{}}.
send_request(Procedure, From,
	     State = #state{socket = Socket,
			    transaction_register = TR,
			    counter = Counter}) ->
    {Tid, PDU} = get_pdu(Counter, Procedure),
    ?debug("Encoding PDU: ~p", [PDU]),
    Bin = apollo_pb:encode_msg(PDU),
    CorrId = encode_unsigned_16(Tid),
    case send(Socket, CorrId, Bin) of
	ok ->
	    true = ets:insert(TR, {CorrId, From}),
	    {noreply, State};
	{error, Reason} ->
	    {reply, {error, Reason}, State}
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
    case ssl:connect(IP, Port, [{active,false}]) of
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
    io:format("Pid: ~p~n",[self()]),
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
	    io:format("SaltedPassword: ~p~n",[SaltedPassword]),
	    io:format("AuthMessage: ~p~n",[AuthMessage]),
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
	    io:format("timeout at wait_server_first_message~n", []),
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
	    io:format("ServerSignature: ~p ~n",[Else]),
	    {error, server_verification}
    end.

-spec handle_server_error(ScramData :: map()) ->
    {error, Reason :: term()}.
handle_server_error(ScramData) ->
    case maps:get('server-error', ScramData, undefined) of
	undefined ->
	    {error, server_verification};
	Error ->
	    io:format("Server Error: ~p~n",[Error]),
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
		    {error, #'Error'{cause = Cause}}}) ->
    {error, Cause}.

-type pbp_table_option() :: {type, type()} |
			    {data_model, data_model()} |
			    {wrapper, #'Wrapper'{}} |
			    {mem_wrapper, #'Wrapper'{}} |
			    {comparator, comparator()} |
			    {time_series, boolean()} |
			    {shards, integer()} |
			    {distributed, boolean()} |
			    {replication_factor, integer()} |
			    {hash_exclude, [string()]}.

-spec make_set_of_table_option(Options :: [table_option()]) ->
    [pbp_table_option()].
make_set_of_table_option(Options) ->
    make_set_of_table_option(Options, []).

-spec make_set_of_table_option(Options :: [table_option()],
			       Acc :: [pbp_table_option()]) ->
    [pbp_table_option()].
make_set_of_table_option([], Acc) ->
    lists:reverse(Acc);
make_set_of_table_option([{type, T}|Rest], Acc) ->
    make_set_of_table_option(Rest, [translate_options({type, T}) | Acc]);
make_set_of_table_option([{data_model, DT}|Rest], Acc) ->
    make_set_of_table_option(Rest, [{data_model, DT} | Acc]);
make_set_of_table_option([{wrapper, {NB, TM, SM}}|Rest], Acc) ->
    Wrapper = #'Wrapper'{num_of_buckets = NB,
			 time_margin = TM,
			 size_margin = SM},
    make_set_of_table_option(Rest, [{wrapper, Wrapper} | Acc]);
make_set_of_table_option([{mem_wrapper, {NB, TM, SM}}|Rest], Acc) ->
    Wrapper = #'Wrapper'{num_of_buckets = NB,
			 time_margin = TM,
			 size_margin = SM},
    make_set_of_table_option(Rest, [{mem_wrapper, Wrapper} | Acc]);
make_set_of_table_option([{comparator, C}|Rest], Acc) ->
    make_set_of_table_option(Rest, [{comparator, C} | Acc]);
make_set_of_table_option([{time_series, T}|Rest], Acc) ->
    make_set_of_table_option(Rest, [{time_series, T} | Acc]);
make_set_of_table_option([{shards, S}|Rest], Acc) ->
    make_set_of_table_option(Rest, [{shards, S} | Acc]);
make_set_of_table_option([{distributed, D}|Rest], Acc) ->
    make_set_of_table_option(Rest, [{distributed, D} | Acc]);
make_set_of_table_option([{replication_factor, RF}|Rest], Acc) ->
    make_set_of_table_option(Rest, [{replication_factor, RF} | Acc]);
make_set_of_table_option([{hash_exclude, HE}|Rest], Acc) ->
    make_set_of_table_option(Rest, [{hash_exclude, HE} | Acc]);
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
translate_options({type, ets_leveldb}) ->
    {type, etsLeveldb};
translate_options({type, leveldb_wrapped}) ->
    {type, leveldbWrapped};
translate_options({type, ets_leveldb_wrapped}) ->
    {type, etsLeveldbWrapped};
translate_options(Option) ->
    Option.

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
			 treshold = <<>>,
			 set_value = <<>>};
make_update_instruction({increment, T, S}) ->
    #'UpdateInstruction'{instruction = 'INCREMENT',
			 treshold = binary:encode_unsigned(T, big),
			 set_value = binary:encode_unsigned(S, big)};
make_update_instruction(overwrite) ->
    #'UpdateInstruction'{instruction = 'OVERWRITE',
			 treshold = <<>>,
			 set_value = <<>>}.
