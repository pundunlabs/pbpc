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

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
         terminate/2,
	 code_change/3,
	 handle_incomming_data/2]).

-include("gb_log.hrl").
-include("pbpc.hrl").
-include("APOLLO-PDU-Descriptions.hrl").

-record(state, {socket,
		transaction_register,
		counter}).

-record(request, {transactionId,
		  pdu,
		  from}).

-define(TID_THRESHOLD, 65535).

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
%% Handle the received response messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_incomming_data(Data :: binary(),
			    TransactionRegister :: pid())->
    ok.
handle_incomming_data(Data, TransactionRegister) ->
    {ok, PDU = #'APOLLO-PDU'{transactionId = Tid}} = pbpc_lib:decode(Data),
    ?debug("Decoded PDU: ~p", [PDU]),
    case lookup_request(TransactionRegister, Tid) of
	{ok, Req = #request{from = Client}} ->
	    Response = get_return_value(PDU),
	    gen_server:reply(Client, Response),
	    unregister_request(TransactionRegister, Req);
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
    case connect(IP, Port, Username, Password) of
	{ok, Socket} ->
	    TR = ets:new(transaction_register, [set, public, {keypos, 2}]),
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
handle_call({create_table, TableName, KeyDef, ColumnsDef,
	    IndexesDef, Options}, From, State) ->
    P = #'CreateTable'{tableName = TableName,
		       keys = KeyDef,
		       columns = ColumnsDef,
		       indexes = IndexesDef,
		       tableOptions = make_set_of_table_option(Options)},
    send_request({createTable, P}, From, State);
handle_call({delete_table, TableName}, From, State) ->
    P = #'DeleteTable'{tableName = TableName},
    send_request({deleteTable, P}, From, State);
handle_call({open_table, TableName}, From, State) ->
    P = #'OpenTable'{tableName = TableName},
    send_request({openTable, P}, From, State);
handle_call({close_table, TableName}, From, State) ->
    P = #'CloseTable'{tableName = TableName},
    send_request({closeTable, P}, From, State);
handle_call({table_inf, TableName}, From, State) ->
    P = #'TableInfo'{tableName = TableName},
    send_request({tableInfo, P}, From, State);
handle_call({table_info, TableName, Attributes}, From, State) ->
    P = #'TableInfo'{tableName = TableName,
		     attributes = Attributes},
    send_request({tableInfo, P}, From, State);
handle_call({read, TableName, Key}, From, State) ->
    P = #'Read'{tableName = TableName,
		key = make_seq_of_fields(Key)},
    send_request({read, P}, From, State);
handle_call({write, TableName, Key, Columns}, From, State) ->
    P = #'Write'{tableName = TableName,
		 key = make_seq_of_fields(Key),
		 columns = make_seq_of_fields(Columns)},
    send_request({write, P}, From, State);
handle_call({delete, TableName, Key}, From, State) ->
    P = #'Delete'{tableName = TableName,
		  key = make_seq_of_fields(Key)},
    send_request({delete, P}, From, State);
handle_call({read_range, TableName, KeyRange, Chunk}, From, State) ->
    P = #'ReadRange'{tableName = TableName,
		     keyRange = make_keyrange(KeyRange),
		     limit = Chunk},
    send_request({readRange, P}, From, State);
handle_call({read_range_n, TableName, StartKey, N}, From, State) ->
    P = #'ReadRangeN'{tableName = TableName,
		      startKey = make_seq_of_fields(StartKey),
		      n = N},
    send_request({readRangeN, P}, From, State);
handle_call({batch_write, TableName, DeleteKeys, WriteKvls}, From, State) ->
    P = #'BatchWrite'{tableName = TableName,
		      deleteKeys = make_keys(DeleteKeys),
		      writeKvps = make_kvls(WriteKvls)},
    send_request({batchWrite, P}, From, State);
handle_call({first, TableName}, From, State) ->
    P = #'First'{tableName = TableName},
    send_request({first, P}, From, State);
handle_call({last, TableName}, From, State) ->
    P = #'Last'{tableName = TableName},
    send_request({last, P}, From, State);
handle_call({seek, TableName, Key}, From, State) ->
    P = #'Seek'{tableName = TableName,
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
handle_cast({reply, Req = #request{from = Reply}, PDU},
	    State = #state{transaction_register = TR}) ->
    ?debug("Reply ~p", [PDU]),
    gen_server:reply(Reply, PDU),
    unregister_request(TR, Req),
    {noreply, State};
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
handle_info({ssl, Socket, Data}, State = #state{transaction_register = TR}) ->
    ?debug("Received ssl data: ~p",[Data]),
    spawn_link(?MODULE, handle_incomming_data, [Data, TR]),
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
    PDU = get_pdu(Counter, Procedure),
    ?debug("Encoding PDU: ~p", [PDU]),
    Bin = pbpc_lib:encode(PDU),
    case send(Socket, Bin) of
	ok ->
	    ok = register_request(TR, PDU, From),
	    {noreply, State};
	{error, Reason} ->
	    {reply, {error, Reason}, State}
    end.

-spec send(Socket :: port(),
	   BinData :: term()) ->
    ok | {error, Reason :: term()}.
send(Socket, {ok, Bin}) when is_binary(Bin) ->
    case ssl:send(Socket, Bin) of
	ok ->
	    ok;
	{error, Reason} ->
	    {error, Reason}
    end;
send(_Socket, {error, Reason}) ->
    {error, Reason}.

-spec connect(IP :: string(),
	      Port :: pos_integer(),
	      Username :: string(),
	      Password :: string()) ->
    {ok, Socket :: port()} | {error, Reason :: term()}.
connect(IP, Port, Username, Password) ->
    case ssl:connect(IP, Port, [{active,false}, {packet,0}]) of
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
	    ok = ssl:setopts(Socket, [{active, once}]),
	    ScramData = scramerl_lib:parse(Data),
	    case maps:get(verifier, ScramData, undefined) of
		undefined ->
		    handle_server_error(ScramData);
		Verifier ->
		    verify_server_signature(Socket, Verifier, Map)
	    end
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
    io:format("SaltedPassword: ~p~n",[SaltedPassword]),
    io:format("AuthMessage: ~p~n",[AuthMessage]),
    io:format("Verifier: ~p~n",[Verifier]),
    case scramerl:server_signature(SaltedPassword, AuthMessage) of
	Verifier ->
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

-spec register_request(TR :: pid(),
		       PDU :: #'APOLLO-PDU'{},
		       From :: {Pid :: pid(), Tag :: term()}) ->
    ok.
register_request(TR, PDU = #'APOLLO-PDU'{transactionId = Tid}, From) ->
    ets:insert(TR, #request{transactionId = Tid,
			    pdu = PDU,
			    from = From}),
    ok.

-spec lookup_request(TR :: pid(), Tid :: integer()) ->
    {ok, Req :: #request{}} | {error, not_found}.
lookup_request(TR, Tid) ->
    case ets:lookup(TR, Tid) of
	[Req = #request{}] ->
	    {ok, Req};
	[] ->
	    {error, not_found}
    end.

-spec unregister_request(TR :: pid(),
			 Req :: #request{}) ->
    ok.
unregister_request(TR, Req) ->
    ets:delete_object(TR, Req),
    ok.

-spec get_pdu(Counter :: integer(),
	      Procedure :: {atom(), term()}) ->
    #'APOLLO-PDU'{}.
get_pdu(Counter, Procedure) ->
    #'APOLLO-PDU'{version = get_version(),
		  transactionId = get_transaction_id(Counter),
		  procedure = Procedure}.

-spec get_version() ->
    #'Version'{}.
get_version() ->
    #'Version'{major = 1, minor = 0}.

-spec get_transaction_id(Counter :: integer()) ->
    Tid :: integer().
get_transaction_id(Counter) ->
    ets:update_counter(Counter, transaction_id, {2, 1, ?TID_THRESHOLD, 0}).

-spec get_return_value(PDU :: #'APOLLO-PDU'{}) ->
    Response :: term().
get_return_value(#'APOLLO-PDU'{procedure =
		    {response, #'Response'{result = asn1_NOVALUE}}}) ->
    ok;
get_return_value(#'APOLLO-PDU'{procedure =
		    {response, #'Response'{result = {columns, Columns}}}}) ->
    {ok, strip_fields(Columns)};
get_return_value(#'APOLLO-PDU'{procedure =
		    {response, #'Response'{result =
			{keyColumnsPair,
			    #'KeyColumnsPair'{key = Key,
					      columns = Columns}}}}}) ->
    {ok, {strip_fields(Key), strip_fields(Columns)}};
get_return_value(#'APOLLO-PDU'{procedure =
		    {response, #'Response'{result =
			{keyColumnsList,
			    #'KeyColumnsList'{list = List,
					      continuation = Cont}}}}}) ->
    Kcl = [begin
	    {strip_fields(K), strip_fields(C)}
	   end || #'KeyColumnsPair'{key = K, columns = C} <- List],
    case Cont of
	asn1_NOVALUE ->
	    {ok, Kcl};
	#'Continuation'{complete = true} ->
	    {ok, Kcl, complete};
	#'Continuation'{complete = false, key = Key} ->
	    {ok, Kcl, strip_fields(Key)}
    end;
get_return_value(#'APOLLO-PDU'{procedure =
		    {response, #'Response'{result =
			{propList, Proplist}}}}) ->
    Result = strip_fields(Proplist),
    {ok, Result};
get_return_value(#'APOLLO-PDU'{procedure =
		    {response, #'Response'{result =
			{kcpIt, #'KcpIt'{keyColumnsPair =
					    #'KeyColumnsPair'{key = K,
							      columns = V},
					 it = It}}}}}) ->
    {ok, {strip_fields(K), strip_fields(V)}, It};
get_return_value(#'APOLLO-PDU'{procedure =
		    {error, #'Error'{cause = Cause}}}) ->
    {error, Cause}.

-type pbp_table_option() :: {type, type()} |
			    {dataModel, data_model()} |
			    {wrapper, #'Wrapper'{}} |
			    {memWrapper, #'Wrapper'{}} |
			    {comparator, comparator()} |
			    {timeSeries, boolean()} |
			    {shards, integer()} |
			    {clusters, [#'Cluster'{}]}.

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
    make_set_of_table_option(Rest, [{dataModel, DT} | Acc]);
make_set_of_table_option([{wrapper, {NB, TM, SM}}|Rest], Acc) ->
    Wrapper = #'Wrapper'{numOfBuckets = NB,
			 timeMargin = asn1_optional(TM),
			 sizeMargin = asn1_optional(SM)},
    make_set_of_table_option(Rest, [{wrapper, Wrapper} | Acc]);
make_set_of_table_option([{mem_wrapper, {NB, TM, SM}}|Rest], Acc) ->
    Wrapper = #'Wrapper'{numOfBuckets = NB,
			 timeMargin = asn1_optional(TM),
			 sizeMargin = asn1_optional(SM)},
    make_set_of_table_option(Rest, [{memWrapper, Wrapper} | Acc]);
make_set_of_table_option([{comparator, C}|Rest], Acc) ->
    make_set_of_table_option(Rest, [{comparator, C} | Acc]);
make_set_of_table_option([{time_series, T}|Rest], Acc) ->
    make_set_of_table_option(Rest, [{timeSeries, T} | Acc]);
make_set_of_table_option([{shards, S}|Rest], Acc) ->
    make_set_of_table_option(Rest, [{shards, S} | Acc]);
make_set_of_table_option([{clusters, CList}|Rest], Acc) ->
    Clusters = [#'Cluster'{name = Name,
			  replicationFactor = RF} || {Name, RF} <- CList],
    make_set_of_table_option(Rest, [{clusters, Clusters} | Acc]);
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
    {real, <<V:64/float>>};
make_value(true) ->
    {bool, true};
make_value(false) ->
    {bool, false};
make_value(V) when is_list(V) ->
    case io_lib:printable_unicode_list(V) of
	true ->
	    {string, V};
	false ->
	    ok%%{binary, list_to_binary(V)}
    end;
make_value(undefined) ->
    {null, 'NULL'}.

-spec make_keyrange(KeyRange :: {[{string(), term()}], [{string(), term()}]}) ->
    #'KeyRange'{}.
make_keyrange({Start, End}) ->
    #'KeyRange'{start = make_seq_of_fields(Start),
		'end' = make_seq_of_fields(End)};
make_keyrange(Else) ->
    ?debug("Invalid key range: ~p",[Else]),
    Else.

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

-spec strip_fields(Fields :: [#'Field'{}]) ->
    [{string(), term()}].
strip_fields(Fields) ->
    strip_fields(Fields, []).

-spec strip_fields(Fields :: [#'Field'{}],
		   Acc :: [{string(), term()}]) ->
    [{string(), term()}].
strip_fields([], Acc) ->
    lists:reverse(Acc);
strip_fields([#'Field'{name = N, value = {string, Str}} | Rest], Acc) ->
    strip_fields(Rest, [{N, binary_to_list(Str)} | Acc]);
strip_fields([#'Field'{name = N, value = {double, Bin}} | Rest], Acc) ->
    <<D:64/float>> = Bin,
    strip_fields(Rest, [{N, D} | Acc]);
strip_fields([#'Field'{name = N, value = {null, _}} | Rest], Acc) ->
    strip_fields(Rest, [{N, undefined} | Acc]);
strip_fields([#'Field'{name = N, value = {_,V}} | Rest], Acc) ->
    strip_fields(Rest, [{N, V} | Acc]).

-spec asn1_optional(A :: undefined |
			 asn1_NOVALUE |
			 term()) ->
    A :: term() | asn1_NOVALUE | undefined.
asn1_optional(undefined) ->
    asn1_NOVALUE;
asn1_optional(asn1_NOVALUE) ->
    undefined;
asn1_optional(A) ->
    A.

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
