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
%% Module Description:
%% @end
%%%===================================================================

-module(pbpc).

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
	 index_read/4,
	 index_read/5,
	 list_tables/1]).

-include("pbpc.hrl").

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
    pbpc_session:connect(IP, Port, Username, Password).

%%--------------------------------------------------------------------
%% @doc
%% Disconnect from pundun host by providing Session handler pid.
%% @end
%%--------------------------------------------------------------------
-spec disconnect(Session :: pid()) ->
    ok.
disconnect(Session) ->
    pbpc_session:disconnect(Session).

%%--------------------------------------------------------------------
%% @doc
%% Create table on pundun nodes/cluster.
%% @end
%%--------------------------------------------------------------------
-spec create_table(Session :: pid(), TabName :: string(),
		   KeyDef :: [atom()], Options :: [table_option()])->
    ok | {error, Reason :: term()}.
create_table(Session, TabName, KeyDef, Options) ->
    pbpc_session:create_table(Session, TabName, KeyDef, Options).

%%--------------------------------------------------------------------
%% @doc
%% Delete table from pundun nodes/cluster.
%% @end
%%--------------------------------------------------------------------
-spec delete_table(Session :: pid(), TabName :: string())->
    ok | {error, Reason :: term()}.
delete_table(Session, TabName) ->
    pbpc_session:delete_table(Session, TabName).

%%--------------------------------------------------------------------
%% @doc
%% Open table a pundun table.
%% @end
%%--------------------------------------------------------------------
-spec open_table(Session :: pid(), TabName :: string())->
    ok | {error, Reason :: term()}.
open_table(Session, TabName) ->
    pbpc_session:open_table(Session, TabName).

%%--------------------------------------------------------------------
%% @doc
%% Close a pundun table.
%% @end
%%--------------------------------------------------------------------
-spec close_table(Session :: pid(), TabName :: string())->
    ok | {error, Reason :: term()}.
close_table(Session, TabName) ->
    pbpc_session:close_table(Session, TabName).

%%--------------------------------------------------------------------
%% @doc
%% Get table information for all attributes.
%% @end
%%--------------------------------------------------------------------
-spec table_info(Session :: pid(),
		 TabName :: string()) ->
    [{atom(), term()}] | {error, Reason :: term()}.
table_info(Session, TabName) ->
    pbpc_session:table_info(Session, TabName).

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
    pbpc_session:table_info(Session, TabName, Attributes).

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
    pbpc_session:read(Session, TabName, Key).

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
    pbpc_session:write(Session, TabName, Key, Columns).

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
    pbpc_session:update(Session, TabName, Key, Op).

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
    pbpc_session:delete(Session, TabName, Key).

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
    pbpc_session:read_range(Session, TabName, StartKey, EndKey, Chunk).

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
    pbpc_session:read_range_n(Session, TabName, StartKey, N).

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
    pbpc_session:batch_write(Session, TabName, DeleteKeys, WriteKvps).

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
    pbpc_session:first(Session, TabName).

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
    pbpc_session:last(Session, TabName).

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
    pbpc_session:seek(Session, TabName, Key).

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
    pbpc_session:next(Session, Ref).

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
    pbpc_session:prev(Session, Ref).

%%--------------------------------------------------------------------
%% @doc
%% Add index on given columns for the table.
%% @end
%%--------------------------------------------------------------------
-spec add_index(Session :: pid(),
		TabName :: string(),
		Columns :: [string()]) ->
    ok | {error, Reason :: term()}.
add_index(Session, TabName, Columns) ->
    pbpc_session:add_index(Session, TabName, Columns).

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
    pbpc_session:remove_index(Session, TabName, Columns).

%%--------------------------------------------------------------------
%% @doc
%% Index read on given tables column.
%% @end
%%--------------------------------------------------------------------
-spec index_read(Session :: pid(),
		 TabName :: string(),
		 Column :: string(),
		 Term :: string()) ->
    {ok, [posting()]} | {error, Reason :: term()}.
index_read(Session, TabName, Column, Term) ->
    pbpc_session:index_read(Session, TabName, Column, Term, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Index read on given tables column,
%% fetch max limit number of postings.
%% @end
%%--------------------------------------------------------------------
-spec index_read(Session :: pid(),
		 TabName :: string(),
		 Column :: string(),
		 Term :: string(),
		 Filter :: posting_filter()) ->
    {ok, [posting()]} | {error, Reason :: term()}.
index_read(Session, TabName, Column, Term, Filter) when is_map(Filter) ->
    pbpc_session:index_read(Session, TabName, Column, Term, Filter);
index_read(Session, TabName, Column, Term, _) ->
    pbpc_session:index_read(Session, TabName, Column, Term, #{}).

%%--------------------------------------------------------------------
%% @doc
%% List existing tables on the pudun node.
%% @end
%%--------------------------------------------------------------------
-spec list_tables(Session :: pid()) ->
    {ok, [string()]} | {error, Reason :: term()}.
list_tables(Session) ->
    pbpc_session:list_tables(Session).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% End of Module.
