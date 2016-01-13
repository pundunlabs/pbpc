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
%% @title
%% @doc
%% Module Description:
%% @end
%%%===================================================================

-module(pbpc_lib).

-export([encode/1,
	 decode/1]).

-include("APOLLO-PDU-Descriptions.hrl").

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Encode a Apollo Protocol PDU.
%% @end
%%--------------------------------------------------------------------
-spec encode(Data :: term())->
    {ok, Bin :: binary()}.
encode(Data) ->
    'APOLLO-PDU-Descriptions':encode('APOLLO-PDU', Data).

%%--------------------------------------------------------------------
%% @doc
%% Decode a Apollo Protocol PDU.
%% @end
%%--------------------------------------------------------------------
-spec decode(Bin :: term())->
    {ok, Data :: term()}.
decode(Bin) ->
    'APOLLO-PDU-Descriptions':decode('APOLLO-PDU', Bin).

