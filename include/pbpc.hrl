-type field_name() :: string().
-type key() :: [{field_name(), term()}].
-type key_range() :: {key(), key()}.
-type value() :: term().
-type kvp() :: {key(), value()}.
-type column() :: {field_name(), term()}.

-type update_threshold() :: pos_integer().
-type update_setvalue() :: pos_integer().
-type update_instruction() :: increment |
                              {increment, update_threshold(), update_setvalue()} |
			      overwrite.

-type update_data() :: pos_integer() | term().
-type update_default() :: pos_integer() | term().
-type update_op() :: [
		      {field_name(), update_instruction(), update_data()} |
                      {field_name(), update_instruction(), update_data(), update_default()}
		     ].

-type type() :: leveldb |
                mem_leveldb |
		leveldb_wrapped |
		mem_leveldb_wrapped |
		leveldb_tda |
		mem_leveldb_tda.

-type data_model() :: kv | array | map.
-type num_of_buckets() :: pos_integer().

-type time_margin() :: {seconds, pos_integer()} |
                       {minutes, pos_integer()} |
		       {hours, pos_integer()} |
		       undefined.

-type size_margin() :: {megabytes, pos_integer()} |
                       {gigabytes, pos_integer()} |
		       undefined.

-type time_unit() :: second | millisecond | microsecond | nanosecond.

-type hashing_method() :: virtual_nodes | consistent | uniform | rendezvous.

-record(wrapper, {num_of_buckets :: pos_integer(),
                  time_margin :: time_margin(),
		  size_margin :: size_margin()}).

-record(tda, {num_of_buckets :: pos_integer(),
	      time_margin :: time_margin(),
	      ts_field :: string(),
	      precision :: time_unit()}).

-type comparator() ::  descending | ascending.

-type table_option() :: {type, type()} |
			{data_model, data_model()} |
			{wrapper, #wrapper{}} |
			{mem_wrapper, #wrapper{}} |
			{comparator, comparator()} |
			{time_series, boolean()} |
			{shards, integer()} |
			{distributed, boolean()} |
			{replication_factor, integer()} |
			{hash_exclude, [string()]} |
			{hashing_method, hashing_method()} |
			{tda, #tda{}}.
