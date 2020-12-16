-module(ar_storage_names).

-export([list/0]).

%% @doc The size in bits of the key prefix used in prefix bloom filter
%% when looking up chunks by offsets from kv database.
%% 29 bytes of the prefix correspond to the 16777216 (16 Mib) max distance
%% between the keys with the same prefix. The prefix should be bigger than
%% max chunk size (256 KiB) so that the chunk in question is likely to be
%% found in the filter and smaller than an SST table (200 MiB) so that the
%% filter lookup can narrow the search down to a single table. @end
-define(OFFSET_KEY_PREFIX_BITSIZE, 232).


-define(BASE_OPTS , [
		{max_open_files, 1000000},
		{write_buffer_size, 256 * 1024 * 1024}, % 256 MiB per memtable.
		{target_file_size_base, 256 * 1024 * 1024}, % 256 MiB per SST file.
		{enable_pipelined_write, true},
		%% 10 files in L1 to make L1 == L0 as recommended by the
		%% RocksDB guide https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide.
		{max_bytes_for_level_base, 10 * 256 * 1024 * 1024}
]).

-define(BLOOM_FILTER_OPTS , [
		{block_based_table_options, [
			{cache_index_and_filter_blocks, true}, % Keep bloom filters in memory.
			{bloom_filter_policy, 10} % ~1% false positive probability.
		]},
		{optimize_filters_for_hits, true}
]).

-define(PREFIX_BLOOM_FILTER_OPTS ,
		?BLOOM_FILTER_OPTS ++ [
			{prefix_extractor, {capped_prefix_transform, ?OFFSET_KEY_PREFIX_BITSIZE div 8}}
]).

% @doc Returns list of defined databases. For internal use only
% Definition format:
% { name , opts } for regular
% { name , [ { CFname, CFopt } ] , opts } for column families database
%
list() ->
	[
		{ chunks,
			[
				{default, ?BASE_OPTS},
				{data_chunks_index, ?BASE_OPTS ++ ?PREFIX_BLOOM_FILTER_OPTS},
				{data_missing_chunks_index, ?BASE_OPTS},
				{data_root_index, ?BASE_OPTS ++ ?BLOOM_FILTER_OPTS},
				{data_root_offset_index, ?BASE_OPTS},
				{data_tx_index, ?BASE_OPTS ++ ?BLOOM_FILTER_OPTS},
				{data_tx_offset_index, ?BASE_OPTS},
				{data_disk_pool_chunks_index, ?BASE_OPTS ++ ?BLOOM_FILTER_OPTS},
				{data_migrations_index, ?BASE_OPTS},
				{data_chunk_data_index, ?BASE_OPTS ++ ?BLOOM_FILTER_OPTS}
			],
			[ {create_if_missing, true}, {create_missing_column_families, true} ]
		},

		{ blocks,
			[ {create_if_missing, true}, {files, "blocks"} ]
		},

		{ txs, 
			[ {create_if_missing, true}, {files, "txs"} ]
		}
	].


