-type field_name() :: string().
-type key() :: [{atom(), term()}].
-type key_range() :: {key(), key()}.
-type value() :: term().
-type kvp() :: {key(), value()}.
-type column() :: {atom(), term()}.

-type type() :: leveldb |
                ets_leveldb |
		leveldb_wrapped |
		ets_leveldb_wrapped.

-type data_model() :: binary | array | hash.
-type num_of_buckets() :: pos_integer().

-type time_margin() :: {seconds, pos_integer()} |
                       {minutes, pos_integer()} |
		       {hours, pos_integer()} |
		       undefined.

-type size_margin() :: {megabytes, pos_integer()} |
                       undefined.

-type wrapper() :: {num_of_buckets(), time_margin(), size_margin()}.

-type comparator() ::  descending | ascending.

-type table_option() :: [{type, type()} |
			 {data_model, data_model()} |
			 {wrapper, wrapper()} |
			 {mem_wrapper, wrapper()} |
			 {comparator, comparator()} |
			 {time_series, boolean()} |
			 {shards, integer()} |
			 {clusters, [{string(), pos_integer()}]}].

-type update_treshold() :: pos_integer().
-type update_setvalue() :: pos_integer().
-type update_instruction() :: increment |
                              {increment, update_treshold(), update_setvalue()}
|
                              overwrite.
-type update_data() :: pos_integer() | term().
-type update_default() :: pos_integer() | term().
-type update_op() :: [
                      {field_name(), update_instruction(), update_data()}|
                      {field_name(), update_instruction(), update_data(), update_default()}
                     ].

