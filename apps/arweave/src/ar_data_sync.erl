-module(ar_data_sync).

-behaviour(gen_server).

-export([
	start_link/0,
	join/1,
	add_tip_block/2, add_block/2,
	is_chunk_proof_ratio_attractive/3,
	add_chunk/1, add_chunk/2, add_chunk/3,
	add_data_root_to_disk_pool/3, maybe_drop_data_root_from_disk_pool/3,
	get_chunk/1, get_tx_root/1, get_tx_data/1, get_tx_offset/1,
	get_sync_record/1,
	request_tx_data_removal/1
]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").

-define(SYNC_RANDOM_INTERVAL, 60 * 1000).

%% @doc The state of the server managing data synchronization.
-record(state, {
	%% @doc A set of non-overlapping intervals of global byte offsets ((end, start))
	%% denoting the synced data. End offsets are defined on [1, weave size], start
	%% offsets are defined on [0, weave size).
	%%
	%% The set serves as a compact map of what is synced by the node. No matter
	%% how big the weave is or how much of it the node stores, this record
	%% can remain very small, compared to storing all chunk and transaction identifiers,
	%% whose number can effectively grow unlimited with time.
	sync_record,
	%% @doc Free disk space
	free_space,
	%% @doc connected keeps the network connectivity state
	connected,
	%% @doc The last ?TRACK_CONFIRMATIONS entries of the block index.
	%% Used to determine orphaned data upon startup or chain reorg.
	block_index,
	%% @doc The current weave size. The upper limit for the absolute chunk end offsets.
	weave_size,
	%% @doc A reference to the on-disk key-value storage mapping
	%% absolute_chunk_end_offset -> {chunk_data_index_key, tx_root, data_root, tx_path, chunk_size}
	%% for all synced chunks.
	%%
	%% Chunks themselves and their data_paths are stored separately
	%% in files identified by data_path hashes. This is made to avoid moving
	%% potentially huge data around during chain reorgs.
	%%
	%% The index is used to look up the chunk by a random offset when a peer
	%% asks about it and to look up chunks of a transaction.
	%%
	%% Every time a chunk is written to sync_record, it is also
	%% written to the chunks_index.
	chunks_index,
	%% @doc A reference to the on-disk key-value storage mapping
	%% << data_root, tx_size >> -> tx_root -> absolute_tx_start_offset -> tx_path.
	%%
	%% The index is used to look up tx_root for a submitted chunk and
	%% to compute absolute_chunk_end_offset for the accepted chunk.
	%%
	%% We need the index because users should be able to submit their
	%% data without monitoring the chain, otherwise chain reorganisations
	%% might make the experience very unnerving. The index is NOT used
	%% for serving random chunks therefore it is possible to develop
	%% a lightweight client which would sync and serve random portions
	%% of the weave without maintaining this index.
	%%
	%% The index contains data roots of all stored chunks therefore it is used
	%% to determine if an orphaned data root can be deleted (the same data root
	%% can belong to multiple tx roots). A potential lightweight client without
	%% this index can simply only sync properly confirmed chunks that are extremely
	%% unlikely to be orphaned.
	data_root_index,
	%% @doc A reference to the on-disk key-value storage mapping
	%% absolute_block_start_offset -> {tx_root, block_size, data_root_index_key_set}.
	%% Each key in data_root_index_key_set is a << data_root, tx_size >> binary.
	%%
	%% Used to remove orphaned entries from data_root_index and to determine
	%% tx_root when syncing random offsets of the weave.
	%% data_root_index_key_set may be empty - in this case, the corresponding index entry
	%% is only used to for syncing the weave.
	data_root_offset_index,
	%% @doc A map of pending, orphaned, and recent data roots
	%% << data_root, tx_size >> -> {size, timestamp, tx_id_set}.
	%%
	%% Unconfirmed chunks can be accepted only after their data roots end up in this set.
	%% Each time a pending data root is added to the map the size is set to 0. New chunks
	%% for these data roots are accepted until the corresponding size reaches
	%% config.max_disk_pool_data_root_buffer_mb or the total size of added pending chunks
	%% reaches config.max_disk_pool_buffer_mb. When a data root is orphaned, it's timestamp
	%% is refreshed so that the chunks have chance to be reincluded later.
	%% After a data root expires, the corresponding chunks are removed from
	%% disk_pool_chunks_index and if they are not in data_root_index - from storage.
	%% tx_id_set keeps track of pending transaction identifiers - if all pending transactions
	%% with the << data_root, tx_size >> key are dropped from the mempool, the corresponding
	%% entry is removed from disk_pool_data_roots. When a data root is confirmed, tx_id_set
	%% is set to not_set - from this point on, the key can't be dropped after a mempool drop.
	disk_pool_data_roots,
	%% @doc A reference to the on-disk key value storage mapping
	%% << data_root_timestamp, chunk_data_index_key >> ->
	%%     {relative_chunk_end_offset, chunk_size, data_root, tx_size, chunk_data_index_key}.
	%%
	%% The index is used to keep track of pending, orphaned, and recent chunks.
	%% A periodic process iterates over chunks from earliest to latest, consults
	%% disk_pool_data_roots and data_root_index to decide whether each chunk needs to
	%% be removed from disk as orphaned, reincluded into the weave (by updating chunks_index),
	%% or removed from disk_pool_chunks_index by expiration.
	disk_pool_chunks_index,
	%% @doc The sum of sizes of all pending chunks. When it reaches
	%% ?MAX_DISK_POOL_BUFFER_MB, new chunks with these data roots are rejected.
	disk_pool_size,
	%% @doc One of the keys from disk_pool_chunks_index or the atom "first".
	%% The disk pool is processed chunk by chunk going from the oldest entry to the newest,
	%% trying not to block the syncing process if the disk pool accumulates a lot of orphaned
	%% and pending chunks. The cursor remembers the key after the last processed on the
	%% previous iteration. After reaching the last key in the storage, we go back to
	%% the first one. Not stored.
	disk_pool_cursor,
	%% @doc A reference to the on-disk key value storage mapping
	%% tx_id -> {absolute_end_offset, tx_size}.
	%% It is used to serve transaction data by TXID.
	tx_index,
	%% @doc A reference to the on-disk key value storage mapping
	%% absolute_tx_start_offset -> tx_id. It is used to cleanup orphaned
	%% transactions from tx_index.
	tx_offset_index,
	%% @doc A reference to the on-disk key value storage mapping
	%% << timestamp, data_path_hash >> of the chunks to chunk data.
	%% The motivation to not store chunk data directly in the chunks_index is to save the
	%% space by not storing identical chunks placed under different offsets several time
	%% and to be able to quickly move chunks from the disk pool to the on-chain storage.
	%% The timestamp prefix is used to make the written entries sorted from the start,
	%% to minimize the compaction overhead.
	chunk_data_db,
	%% @doc A reference to the on-disk key value storage mapping migration names to their
	%% stages.
	migrations_index
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Notify the server the node has joined the network on the given block index.
join(BI) ->
	gen_server:cast(?MODULE, {join, BI}).

%% @doc Notify the server about the new tip block.
add_tip_block(BlockTXPairs, RecentBI) ->
	gen_server:cast(?MODULE, {add_tip_block, BlockTXPairs, RecentBI}).

%% @doc The condition which is true if the chunk is too small compared to the proof.
%% Small chunks make syncing slower and increase space amplification. A small chunk
%% is accepted if it is the last chunk of the corresponding transaction - such chunks
%% may be produced by ar_tx:chunk_binary/1, the legacy splitting method used to split
%% v1 data or determine the data root of a v2 tx when data is uploaded via the data field.
%% Due to the block limit we can only get up to 1k such chunks per block.
%% @end
is_chunk_proof_ratio_attractive(ChunkSize, TXSize, DataPath) ->
	DataPathSize = byte_size(DataPath),
	case DataPathSize of
		0 ->
			false;
		_ ->
			case catch ar_merkle:extract_note(DataPath) of
				{'EXIT', _} ->
					false;
				Offset ->
					Offset == TXSize orelse DataPathSize =< ChunkSize
			end
	end.

%% @doc Store the given chunk if the proof is valid.
add_chunk(Proof) ->
	add_chunk(Proof, 5000, do_not_write_to_free_space_buffer).

%% @doc Store the given chunk if the proof is valid.
add_chunk(Proof, Timeout) ->
	add_chunk(Proof, Timeout, do_not_write_to_free_space_buffer).

%% @doc Store the given chunk if the proof is valid.
add_chunk(
	#{
		data_root := DataRoot,
		offset := Offset,
		data_path := DataPath,
		chunk := Chunk,
		data_size := TXSize
	},
	Timeout,
	WriteToFreeSpaceBuffer
) ->
	Command = {add_chunk, DataRoot, DataPath, Chunk, Offset, TXSize, WriteToFreeSpaceBuffer},
	case catch gen_server:call(?MODULE, Command, Timeout) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Notify the server about the new pending data root (added to mempool).
%% The server may accept pending chunks and store them in the disk pool.
%% @end
add_data_root_to_disk_pool(<<>>, _, _) ->
	ok;
add_data_root_to_disk_pool(_, 0, _) ->
	ok;
add_data_root_to_disk_pool(DataRoot, TXSize, TXID) ->
	gen_server:cast(?MODULE, {add_data_root_to_disk_pool, {DataRoot, TXSize, TXID}}).

%% @doc Notify the server the given data root has been removed from the mempool.
maybe_drop_data_root_from_disk_pool(<<>>, _, _) ->
	ok;
maybe_drop_data_root_from_disk_pool(_, 0, _) ->
	ok;
maybe_drop_data_root_from_disk_pool(DataRoot, TXSize, TXID) ->
	gen_server:cast(?MODULE, {maybe_drop_data_root_from_disk_pool, {DataRoot, TXSize, TXID}}).

%% @doc Fetch the chunk containing the given global offset, including right bound,
%% excluding left bound.
get_chunk(Offset) ->
	case ar_ets_intervals:is_inside(data_sync_record, Offset) of
		false ->
			{error, chunk_not_found};
		true ->
			case ets:lookup(?MODULE, chunks_index) of
				[] ->
					{error, not_joined};
				[{_, ChunksIndex}] ->
					[{_, ChunkDataDB}] = ets:lookup(?MODULE, chunk_data_db),
					get_chunk(ChunksIndex, ChunkDataDB, Offset)
			end
	end.

get_chunk(ChunksIndex, ChunkDataDB, Offset) ->
	case get_chunk_by_byte(ChunksIndex, Offset) of
		{error, _} ->
			{error, chunk_not_found};
		{ok, Key, Value} ->
			<< ChunkOffset:?OFFSET_KEY_BITSIZE >> = Key,
			{ChunkDataDBKey, TXRoot, _, TXPath, _, ChunkSize} = binary_to_term(Value),
			case ChunkOffset - Offset >= ChunkSize of
				true ->
					{error, chunk_not_found};
				false ->
					case read_chunk(ChunkDataDB, ChunkDataDBKey) of
						{ok, {Chunk, DataPath}} ->
							Proof = #{
								tx_root => TXRoot,
								chunk => Chunk,
								data_path => DataPath,
								tx_path => TXPath
							},
							{ok, Proof};
						not_found ->
							{error, chunk_not_found};
						{error, Reason} ->
							?LOG_ERROR([
								{event, failed_to_read_chunk},
								{reason, Reason}
							]),
							{error, failed_to_read_chunk}
					end
			end
	end.

%% @doc Fetch the transaction root containing the given global offset, including left bound,
%% excluding right bound.
get_tx_root(Offset) ->
	case ets:lookup(?MODULE, data_root_offset_index) of
		[] ->
			{error, not_joined};
		[{_, DataRootOffsetIndex}] ->
			get_tx_root(DataRootOffsetIndex, Offset)
	end.

get_tx_root(DataRootOffsetIndex, Offset) ->
	case ar_kv:get_prev(DataRootOffsetIndex, << Offset:?OFFSET_KEY_BITSIZE >>) of
		{error, _} ->
			{error, not_found};
		{ok, Key, Value} ->
			<< BlockStartOffset:?OFFSET_KEY_BITSIZE >> = Key,
			{TXRoot, BlockSize, _DataRootIndexKeySet} = binary_to_term(Value),
			{ok, TXRoot, BlockStartOffset, BlockSize}
	end.

%% @doc Fetch the transaction data.
get_tx_data(TXID) ->
	case ets:lookup(?MODULE, tx_index) of
		[] ->
			{error, not_joined};
		[{_, TXIndex}] ->
			[{_, ChunksIndex}] = ets:lookup(?MODULE, chunks_index),
			[{_, ChunkDataDB}] = ets:lookup(?MODULE, chunk_data_db),
			get_tx_data(TXIndex, ChunksIndex, ChunkDataDB, TXID)
	end.

get_tx_data(TXIndex, ChunksIndex, ChunkDataDB, TXID) ->
	case ar_kv:get(TXIndex, TXID) of
		not_found ->
			{error, not_found};
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_get_tx_data}, {reason, Reason}]),
			{error, failed_to_get_tx_data};
		{ok, Value} ->
			{Offset, Size} = binary_to_term(Value),
			case Size > ?MAX_SERVED_TX_DATA_SIZE of
				true ->
					{error, tx_data_too_big};
				false ->
					StartKey = << (Offset - Size):?OFFSET_KEY_BITSIZE >>,
					EndKey = << Offset:?OFFSET_KEY_BITSIZE >>,
					case ar_kv:get_range(ChunksIndex, StartKey, EndKey) of
						{error, Reason} ->
							?LOG_ERROR([
								{event, failed_to_get_chunks_for_tx_data},
								{reason, Reason}
							]),
							{error, not_found};
						{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
							{error, not_found};
						{ok, Map} ->
							get_tx_data_from_chunks(ChunkDataDB, Offset, Size, Map)
					end
			end
	end.

%% @doc Return the global end offset and size for the given transaction.
get_tx_offset(TXID) ->
	case ets:lookup(?MODULE, tx_index) of
		[] ->
			{error, not_joined};
		[{_, TXIndex}] ->
			get_tx_offset(TXIndex, TXID)
	end.

get_tx_offset(TXIndex, TXID) ->
	case ar_kv:get(TXIndex, TXID) of
		{ok, Value} ->
			{ok, binary_to_term(Value)};
		not_found ->
			{error, not_found};
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_read_tx_offset}, {reason, Reason}]),
			{error, failed_to_read_offset}
	end.

%% @doc Return a set of intervals of synced data ranges.
%%
%% Args is a map with he following keys:
%%
%% | ------------- | -------- | ------------ | -------------------------------------------------- |
%% | format        | required | etf or json  | serialize in Erlang Term Format or JSON            |
%% | random_subset | optional | any()        | pick a random subset if the key is present         |
%% | start         | optional | integer()    | pick intervals with right bound >= start           |
%% | limit         | optional | integer()    | the number of intervals to pick                    |
%%
%% ?MAX_SHARED_SYNCED_INTERVALS_COUNT is both the default and the maximum value for limit.
%% If random_subset key is present, a random subset of intervals is picked, the start key is
%% ignored. If random_subset key is not present, the start key must be provided.
%% @end
get_sync_record(Args) ->
	case catch gen_server:call(?MODULE, {get_sync_record, Args}) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Record the metadata of the given block.
add_block(B, SizeTaggedTXs) ->
	gen_server:cast(?MODULE, {add_block, B, SizeTaggedTXs}).

%% @doc Request the removal of the transaction data.
request_tx_data_removal(TXID) ->
	gen_server:cast(?MODULE, {remove_tx_data, TXID}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	?LOG_INFO([{event, ar_data_sync_start}]),
	process_flag(trap_exit, true),
	ar_events:subscribe(network),
	State = init_kv(),
	{SyncRecord, CurrentBI, DiskPoolDataRoots, DiskPoolSize, WeaveSize} =
		read_data_sync_state(),
	ar_ets_intervals:init_from_gb_set(data_sync_record, SyncRecord),
	State2 = State#state{
		sync_record = SyncRecord,
		free_space = ar_storage:get_free_space(),
		connected = ar_network:is_connected(),
		block_index = CurrentBI,
		weave_size = WeaveSize,
		disk_pool_data_roots = DiskPoolDataRoots,
		disk_pool_size = DiskPoolSize,
		disk_pool_cursor = first
	},
	gen_server:cast(?MODULE, sync_random_interval),
	gen_server:cast(?MODULE, update_disk_pool_data_roots),
	gen_server:cast(?MODULE, process_disk_pool_item),
	gen_server:cast(?MODULE, store_sync_state),
	migrate_index(State2),
	{ok, State2}.

handle_cast({migrate, Key = <<"store_data_in_v2_index">>, Cursor}, State) ->
	#state{
		chunks_index = ChunksIndex,
		migrations_index = MigrationsDB
	} = State,
	case ar_kv:cyclic_iterator_move(ChunksIndex, Cursor) of
		none ->
			ok = ar_kv:put(MigrationsDB, Key, <<"complete">>),
			ets:insert(?MODULE, {store_data_in_v2_index_completed}),
			{noreply, State};
		{ok, ChunkKey, Value, NextCursor} ->
			case migrate_chunk(ChunkKey, Value, State) of
				{ok, State2} ->
					case NextCursor of
						first ->
							ok = ar_kv:put(MigrationsDB, Key, <<"complete">>),
							ets:insert(?MODULE, {store_data_in_v2_index_completed}),
							{noreply, State2};
						{seek, NextCursor2} ->
							ok = ar_kv:put(MigrationsDB, Key, NextCursor2),
							gen_server:cast(?MODULE, {migrate, Key, NextCursor}),
							{noreply, State2}
						end;
				{error, Reason} ->
					?LOG_ERROR([
						{event, ar_data_sync_migration_error},
						{key, Key},
						{reason, Reason},
						{cursor,
							case Cursor of
								first ->
									first;
								_ ->
									ar_util:encode(element(2, Cursor))
							end}
					]),
					cast_after(?MIGRATION_RETRY_DELAY_MS, {migrate, Key, Cursor}),
					{noreply, State}
			end
	end;

handle_cast({join, BI}, State) ->
	#state{
		data_root_offset_index = DataRootOffsetIndex,
		disk_pool_data_roots = DiskPoolDataRoots,
		sync_record = SyncRecord,
		block_index = CurrentBI
	} = State,
	[{_, WeaveSize, _} | _] = BI,
	{DiskPoolDataRoots2, SyncRecord2} =
		case {CurrentBI, ar_util:get_block_index_intersection(BI, CurrentBI)} of
			{[], _Intersection} ->
				ok = data_root_offset_index_from_block_index(DataRootOffsetIndex, BI, 0),
				{DiskPoolDataRoots, SyncRecord};
			{_CurrentBI, none} ->
				throw(last_stored_block_index_has_no_intersection_with_the_new_one);
			{_CurrentBI, {{H, Offset, _TXRoot}, _Height}} ->
				PreviousWeaveSize = element(2, hd(CurrentBI)),
				{ok, OrphanedDataRoots} = remove_orphaned_data(State, Offset, PreviousWeaveSize),
				ok = data_root_offset_index_from_block_index(
					DataRootOffsetIndex,
					lists:takewhile(fun({BH, _, _}) -> BH /= H end, BI),
					Offset
				),
				ar_ets_intervals:cut(data_sync_record, Offset),
				SyncRecord3 = ar_intervals:cut(SyncRecord, Offset),
				{
					reset_orphaned_data_roots_disk_pool_timestamps(
						DiskPoolDataRoots,
						OrphanedDataRoots
					),
					SyncRecord3
				}
		end,
	State2 =
		State#state{
			disk_pool_data_roots = DiskPoolDataRoots2,
			sync_record = SyncRecord2,
			weave_size = WeaveSize,
			block_index = lists:sublist(BI, ?TRACK_CONFIRMATIONS)
		},
	ok = store_sync_state(State2),
	{noreply, State2};

handle_cast({add_tip_block, BlockTXPairs, BI}, State) ->
	#state{
		tx_index = TXIndex,
		tx_offset_index = TXOffsetIndex,
		sync_record = SyncRecord,
		weave_size = CurrentWeaveSize,
		disk_pool_data_roots = DiskPoolDataRoots,
		disk_pool_size = DiskPoolSize,
		block_index = CurrentBI
	} = State,
	{BlockStartOffset, Blocks} = pick_missing_blocks(CurrentBI, BlockTXPairs),
	{ok, OrphanedDataRoots} = remove_orphaned_data(State, BlockStartOffset, CurrentWeaveSize),
	{WeaveSize, AddedDataRoots, DiskPoolSize2} = lists:foldl(
		fun ({_BH, []}, Acc) ->
				Acc;
			({_BH, SizeTaggedTXs}, {StartOffset, CurrentAddedDataRoots, CurrentDiskPoolSize}) ->
				{ok, DataRoots} = add_block_data_roots(State, SizeTaggedTXs, StartOffset),
				ok = update_tx_index(TXIndex, TXOffsetIndex, SizeTaggedTXs, StartOffset),
				{StartOffset + element(2, lists:last(SizeTaggedTXs)),
					sets:union(CurrentAddedDataRoots, DataRoots),
					sets:fold(
						fun(Key, Acc) ->
							Acc - element(1, maps:get(Key, DiskPoolDataRoots, {0, noop, noop}))
						end,
						CurrentDiskPoolSize,
						DataRoots
					)}
		end,
		{BlockStartOffset, sets:new(), DiskPoolSize},
		Blocks
	),
	DiskPoolDataRoots2 =
		reset_orphaned_data_roots_disk_pool_timestamps(
			add_block_data_roots_to_disk_pool(DiskPoolDataRoots, AddedDataRoots),
			OrphanedDataRoots
		),
	ar_ets_intervals:cut(data_sync_record, BlockStartOffset),
	SyncRecord2 = ar_intervals:cut(SyncRecord, BlockStartOffset),
	State2 = State#state{
		weave_size = WeaveSize,
		sync_record = SyncRecord2,
		block_index = BI,
		disk_pool_data_roots = DiskPoolDataRoots2,
		disk_pool_size = DiskPoolSize2
	},
	ok = store_sync_state(State2),
	{noreply, State2};

handle_cast({add_data_root_to_disk_pool, {DataRoot, TXSize, TXID}}, State) ->
	#state{ disk_pool_data_roots = DiskPoolDataRoots } = State,
	Key = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	UpdatedDiskPoolDataRoots = maps:update_with(
		Key,
		fun ({_, _, not_set} = V) ->
				V;
			({S, T, TXIDSet}) ->
				{S, T, sets:add_element(TXID, TXIDSet)}
		end,
		{0, os:system_time(microsecond), sets:from_list([TXID])},
		DiskPoolDataRoots
	),
	{noreply, State#state{ disk_pool_data_roots = UpdatedDiskPoolDataRoots }};

handle_cast({maybe_drop_data_root_from_disk_pool, {DataRoot, TXSize, TXID}}, State) ->
	#state{
		disk_pool_data_roots = DiskPoolDataRoots,
		disk_pool_size = DiskPoolSize
	} = State,
	Key = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	{UpdatedDiskPoolDataRoots, UpdatedDiskPoolSize} = case maps:get(Key, DiskPoolDataRoots, not_found) of
		not_found ->
			{DiskPoolDataRoots, DiskPoolSize};
		{_, _, not_set} ->
			{DiskPoolDataRoots, DiskPoolSize};
		{Size, T, TXIDs} ->
			case sets:subtract(TXIDs, sets:from_list([TXID])) of
				TXIDs ->
					{DiskPoolDataRoots, DiskPoolSize};
				UpdatedSet ->
					case sets:size(UpdatedSet) of
						0 ->
							{maps:remove(Key, DiskPoolDataRoots), DiskPoolSize - Size};
						_ ->
							{DiskPoolDataRoots#{ Key => {Size, T, UpdatedSet} }, DiskPoolSize}
					end
			end
	end,
	{noreply, State#state{
		disk_pool_data_roots = UpdatedDiskPoolDataRoots,
		disk_pool_size = UpdatedDiskPoolSize
	}};

handle_cast({add_block, B, SizeTaggedTXs}, State) ->
	add_block(B, SizeTaggedTXs, State),
	{noreply, State};

handle_cast(check_space, State)
  			when State#state.free_space < ?DISK_DATA_BUFFER_SIZE ->
	FreeSpace = ar_storage:get_free_space(),
	cast_after(ar_disksup:get_disk_space_check_frequency(), check_space),
	{noreply, State#state{free_space = FreeSpace}};

handle_cast(check_space, State) ->
	?LOG_DEBUG("check space"),
	FreeSpace = ar_storage:get_free_space(),
	gen_server:cast(?MODULE, sync_random_interval),
	{noreply, State#state{free_space = FreeSpace}};

handle_cast(sync_random_interval, State)
			when not State#state.connected ->
	?LOG_DEBUG("Data Sync paused. Waiting for the network..."),
	{noreply, State};

handle_cast(sync_random_interval, State)
			when State#state.free_space < ?DISK_DATA_BUFFER_SIZE ->
	?LOG_DEBUG("sync random interval. not enough space"),
	cast_after(ar_disksup:get_disk_space_check_frequency(), check_space),
	Msg =
		"The node has stopped syncing data - the available disk space is"
		" less than ~s. Add more disk space if you wish to store more data.~n",
	ar:console(Msg, [ar_util:bytes_to_mb_string(?DISK_DATA_BUFFER_SIZE)]),
	?LOG_INFO([{event, ar_data_sync_stopped_syncing}, {reason, not_enought_disk_space}]),
	{noreply, State};


%% Pick a random not synced interval and sync it.
handle_cast(sync_random_interval, State) ->
	#state{
		sync_record = SyncRecord,
		weave_size = WeaveSize
	} = State,
	?LOG_DEBUG("sync random interval"),
	FreeSpace = ar_storage:get_free_space(),
	PeerSyncRecords = ar_network:get_sync_records(),
	% shuffle and limit the list of PeersSyncRecord?
	case get_random_interval(SyncRecord, PeerSyncRecords, WeaveSize) of
		Chunks when length(Chunks) > 0 ->
			% Chunks = [{PeerID, LeftBound, RightBound} | ...]
			?LOG_DEBUG("Sync data. Chunks (intervals): ~p", [Chunks]),
			lists:map(fun({PeerID, LeftBound, RightBound}) ->
				gen_server:cast(self(), {sync_chunk, PeerID, LeftBound, RightBound})
			end, Chunks),
			{noreply, State#state{free_space = FreeSpace}};
		E ->
			?LOG_DEBUG("Sync data. get_random_interval ~p", [E]),
			timer:apply_after(
				?SYNC_RANDOM_INTERVAL,
				gen_server,
				cast,
				[?MODULE, sync_random_interval]
			),
			{noreply, State#state{free_space = FreeSpace}}
	end;

handle_cast({sync_chunk, _, Byte, RightBound}, State) when Byte >= RightBound ->
	?LOG_DEBUG("sync chunk. reach the bound"),
	gen_server:cast(self(), sync_random_interval),
	{noreply, State};
handle_cast({sync_chunk, PeerID, Byte, RightBound}, State) ->
	?LOG_DEBUG("sync chunk. spawn get_chunks ~p ~p", [PeerID, Byte]),
	spawn(
		fun() ->
			Byte2 = ar_tx_blacklist:get_next_not_blacklisted_byte(Byte + 1),
			case ar_network:get_chunks([{PeerID, Byte2}]) of
				error ->
					gen_server:cast(?MODULE, sync_random_interval);
				Proofs ->
					case maps:get(PeerID, Proofs, undefined) of
						undefined ->
							?LOG_ERROR("internal error in sync_chank fetching");
						not_found ->
							?LOG_DEBUG("Sync data. chunk offset ~p on ~p is not found", [Byte2, PeerID]);
						error ->
							?LOG_ERROR("Sync data. error getting chunk offset ~p on ~p", [Byte2, PeerID]);
						Proof ->
							gen_server:cast(
								?MODULE,
								{
									store_fetched_chunk,
									PeerID,
									Byte2 - 1,
									RightBound,
									Proof
								}
							)
					end
			end
		end
	),
	{noreply, State};

handle_cast({store_fetched_chunk, PeerID, Byte, RightBound, Proof}, State) ->
	#state{
		data_root_offset_index = DataRootOffsetIndex
	} = State,
	?LOG_DEBUG("store fetched chunkd. ~p ~p", [PeerID, Byte]),
	#{ data_path := DataPath, tx_path := TXPath, chunk := Chunk } = Proof,
	{ok, Key, Value} = ar_kv:get_prev(DataRootOffsetIndex, << Byte:?OFFSET_KEY_BITSIZE >>),
	<< BlockStartOffset:?OFFSET_KEY_BITSIZE >> = Key,
	{TXRoot, BlockSize, DataRootIndexKeySet} = binary_to_term(Value),
	Offset = Byte - BlockStartOffset,
	case validate_proof(TXRoot, TXPath, DataPath, Offset, Chunk, BlockSize) of
		false ->
			gen_server:cast(?MODULE, sync_random_interval),
			{noreply, State};
		{true, DataRoot, TXStartOffset, ChunkEndOffset, TXSize} ->
			AbsoluteTXStartOffset = BlockStartOffset + TXStartOffset,
			DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
			ChunkSize = byte_size(Chunk),
			case is_chunk_proof_ratio_attractive(ChunkSize, TXSize, DataPath) of
				false ->
					gen_server:cast(?MODULE, sync_random_interval),
					{noreply, State};
				true ->
					Byte2 = Byte + ChunkSize,
					gen_server:cast(?MODULE, {sync_chunk, PeerID, Byte2, RightBound}),
					store_fetched_chunk(
						{
							DataRoot,
							DataRootKey,
							Key,
							DataRootIndexKeySet,
							AbsoluteTXStartOffset,
							TXPath,
							TXRoot,
							TXSize,
							BlockSize,
							DataPath,
							ChunkEndOffset,
							Chunk
						},
						State
					)
			end
	end;

handle_cast(process_disk_pool_item, State) ->
	#state{
		disk_pool_cursor = Cursor,
		disk_pool_chunks_index = DiskPoolChunksIndex
	} = State,
	{Cursor2, SkipTimestamp} =
		case Cursor of
			{skip_timestamp, SkipTimestamp2} ->
				{{seek, << (SkipTimestamp2 + 1):256 >>}, SkipTimestamp2};
			_ ->
				{Cursor, none}
		end,
	case ar_kv:cyclic_iterator_move(DiskPoolChunksIndex, Cursor2) of
		{ok, Key, Value, NextCursor} ->
			<< Timestamp:256, _/binary >> = Key,
			case SkipTimestamp /= none andalso Timestamp =< SkipTimestamp of
				true ->
					%% We completed the full scan, make a break.
					timer:apply_after(
						?DISK_POOL_SCAN_FREQUENCY_MS,
						gen_server,
						cast,
						[self(), process_disk_pool_item]
					),
					{noreply, State#state{ disk_pool_cursor = first }};
				_ ->
					timer:apply_after(
						case NextCursor of
							first ->
								?DISK_POOL_SCAN_FREQUENCY_MS;
							_ ->
								0
						end,
						gen_server,
						cast,
						[self(), process_disk_pool_item]
					),
					process_disk_pool_item(State, Key, Value, NextCursor)
			end;
		none ->
			%% Since we are not persisting the number of chunks in the disk pool metric on
			%% every update, it can go out of sync with the actual value. Here is one place
			%% where we can reset it if we detect the whole column has been traversed.
			prometheus_gauge:set(disk_pool_chunks_count, 0),
			timer:apply_after(
				?DISK_POOL_SCAN_FREQUENCY_MS,
				gen_server,
				cast,
				[self(), process_disk_pool_item]
			),
			{noreply, State}
	end;

handle_cast(update_disk_pool_data_roots, State) ->
	#state{ disk_pool_data_roots = DiskPoolDataRoots } = State,
	Now = os:system_time(microsecond),
	UpdatedDiskPoolDataRoots = maps:filter(
		fun(_, {_, Timestamp, _}) ->
			Timestamp + ar_meta_db:get(disk_pool_data_root_expiration_time_us) > Now
		end,
		DiskPoolDataRoots
	),
	DiskPoolSize = maps:fold(
		fun(_, {Size, _, _}, Acc) ->
			Acc + Size
		end,
		0,
		UpdatedDiskPoolDataRoots
	),
	prometheus_gauge:set(pending_chunks_size, DiskPoolSize),
	timer:apply_after(
		?REMOVE_EXPIRED_DATA_ROOTS_FREQUENCY_MS,
		gen_server,
		cast,
		[self(), update_disk_pool_data_roots]
	),
	{noreply, State#state{
		disk_pool_data_roots = UpdatedDiskPoolDataRoots,
		disk_pool_size = DiskPoolSize
	}};

handle_cast({remove_tx_data, TXID}, State) ->
	#state{ tx_index = TXIndex } = State,
	case ar_kv:get(TXIndex, TXID) of
		{ok, Value} ->
			{End, Size} = binary_to_term(Value),
			Start = End - Size,
			gen_server:cast(?MODULE, {remove_tx_data, TXID, Size, End, Start + 1}),
			{noreply, State};
		not_found ->
			?LOG_WARNING([
				{event, tx_offset_not_found},
				{tx, ar_util:encode(TXID)}
			]),
			{noreply, State};
		{error, Reason} ->
			?LOG_ERROR([
				{event, failed_to_fetch_blacklisted_tx_offset},
				{tx, ar_util:encode(TXID)},
				{reason, Reason}
			]),
			{noreply, State}
	end;

handle_cast({remove_tx_data, TXID, TXSize, End, Cursor}, State) when Cursor > End ->
	ar_tx_blacklist:notify_about_removed_tx_data(TXID, End, End - TXSize),
	{noreply, State};
handle_cast({remove_tx_data, TXID, TXSize, End, Cursor}, State) ->
	#state{
		chunks_index = ChunksIndex,
		data_root_index = DataRootIndex,
		sync_record = SyncRecord
	} = State,
	case get_chunk_by_byte(ChunksIndex, Cursor) of
		{ok, Key, Chunk} ->
			<< AbsoluteEndOffset:?OFFSET_KEY_BITSIZE >> = Key,
			case AbsoluteEndOffset > End of
				true ->
					ar_tx_blacklist:notify_about_removed_tx_data(TXID, End, End - TXSize),
					{noreply, State};
				false ->
					{ChunkDataKey, _, DataRoot, _, ChunkOffset, ChunkSize} =
						binary_to_term(Chunk),
					AbsoluteStartOffset = AbsoluteEndOffset - ChunkSize,
					ar_ets_intervals:delete(
						data_sync_record,
						AbsoluteEndOffset,
						AbsoluteStartOffset
					),
					SyncRecord2 =
						ar_intervals:delete(SyncRecord, AbsoluteEndOffset, AbsoluteStartOffset),
					State2 =
						State#state{
							sync_record = SyncRecord2
						},
					ok = store_sync_state(State2),
					ok = ar_kv:delete(ChunksIndex, Key),
					DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
					case ar_kv:get(DataRootIndex, DataRootKey) of
						{ok, DataRootIndexValue} ->
							DataRootMap = binary_to_term(DataRootIndexValue),
							AbsoluteEndOffsets =
								get_absolute_chunk_offsets(
									ChunkOffset,
									data_root_index_iterator(DataRootMap),
									State2
								),
							case AbsoluteEndOffsets of
								[AbsoluteEndOffset] ->
									%% The removed offset is the only reference to
									%% the chunk so we can remove the chunk itself.
									delete_chunk(ChunkDataKey, State2);
								_ ->
									do_not_remove_chunk
							end;
						not_found ->
							?LOG_ERROR([
								{event, data_root_not_found_in_data_root_index},
								{data_root, ar_util:encode(DataRoot)},
								{tx_size, TXSize}
							]);
						{error, Reason} ->
							?LOG_ERROR([
								{event, failed_to_fetch_data_root},
								{data_root, ar_util:encode(DataRoot)},
								{tx_size, TXSize},
								{reason, Reason}
							])
					end,
					gen_server:cast(
						?MODULE,
						{remove_tx_data, TXID, TXSize, End, AbsoluteEndOffset + 1}
					),
					{noreply, State2}
			end;
		{error, invalid_iterator} ->
			%% get_chunk_by_byte looks for a key with the same prefix or the next prefix.
			%% Therefore, if there is no such key, it does not make sense to look for any
			%% key smaller than the prefix + 2 in the next iteration.
			PrefixSpaceSize =
				trunc(math:pow(2, ?OFFSET_KEY_BITSIZE - ?OFFSET_KEY_PREFIX_BITSIZE)),
			NextCursor = ((Cursor div PrefixSpaceSize) + 2) * PrefixSpaceSize,
			gen_server:cast(?MODULE, {remove_tx_data, TXID, TXSize, End, NextCursor}),
			{noreply, State};
		{error, Reason} ->
			?LOG_ERROR([
				{event, tx_data_removal_aborted_since_failed_to_query_chunk},
				{offset, Cursor},
				{reason, Reason}
			]),
			{noreply, State}
	end;

handle_cast(store_sync_state, State) ->
	ok = store_sync_state(State),
	cast_after(?STORE_STATE_FREQUENCY_MS, store_sync_state),
	{noreply, State}.

handle_call({add_chunk, _, _, _, _, _, _} = Msg, _From, State) ->
	{add_chunk, DataRoot, DataPath, Chunk, Offset, TXSize, WriteToFreeSpaceBuffer} = Msg,
	case WriteToFreeSpaceBuffer == write_to_free_space_buffer orelse have_free_space() of
		false ->
			{reply, {error, disk_full}, State};
		true ->
			case add_chunk(DataRoot, DataPath, Chunk, Offset, TXSize, State) of
				{ok, UpdatedState} ->
					{reply, ok, UpdatedState};
				{{error, Reason}, MaybeUpdatedState} ->
					?LOG_WARNING([
						{event, ar_data_sync_failed_to_store_chunk},
						{reason, Reason},
						{data_root, ar_util:encode(DataRoot)}
					]),
					{reply, {error, Reason}, MaybeUpdatedState}
			end
	end;

handle_call({get_sync_record, Args}, _From, State) ->
	#state{
		sync_record = SyncRecord
	} = State,
	Limit =
		min(
			maps:get(limit, Args, ?MAX_SHARED_SYNCED_INTERVALS_COUNT),
			?MAX_SHARED_SYNCED_INTERVALS_COUNT
		),
	{reply, {ok, ar_intervals:serialize(Args#{ limit => Limit }, SyncRecord)}, State};

handle_call(get_sync_record_etf, _From, #state{ sync_record = SyncRecord } = State) ->
	Limit = ?MAX_SHARED_SYNCED_INTERVALS_COUNT,
	{reply, {ok, ar_intervals:to_etf(SyncRecord, Limit)}, State};

handle_call(get_sync_record_json, _From, #state{ sync_record = SyncRecord } = State) ->
	Limit = ?MAX_SHARED_SYNCED_INTERVALS_COUNT,
	{reply, {ok, ar_intervals:to_json(SyncRecord, Limit)}, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%									 {noreply, State, Timeout} |
%%									 {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({event, network, disconnected}, State) ->
	?LOG_DEBUG("Sync Data. Network disconnected. Stop sync random interval"),
	{noreply, State#state{connected = false}};
handle_info({event, network, connected}, State) ->
	?LOG_DEBUG("Sync Data. Network connected. Start sync random interval"),
	gen_server:cast(?MODULE, sync_random_interval),
	{noreply, State#state{connected = true}};

handle_info(_Message, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(Reason, State) ->
	#state{ chunks_index = {DB, _}, chunk_data_db = ChunkDataDB } = State,
	?LOG_INFO([{event, terminate}, {module, ?MODULE}, {reason, Reason}]),
	ok = store_sync_state(State),
	ar_kv:close(DB),
	ar_kv:close(ChunkDataDB).

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_chunk_by_byte(ChunksIndex, Byte) ->
	ar_kv:get_next_by_prefix(
		ChunksIndex,
		?OFFSET_KEY_PREFIX_BITSIZE,
		?OFFSET_KEY_BITSIZE,
		<< Byte:?OFFSET_KEY_BITSIZE >>
	).

read_chunk(ChunkDataDB, ChunkDataDBKey) ->
	case ar_kv:get(ChunkDataDB, ChunkDataDBKey) of
		not_found ->
			case ets:member(?MODULE, store_data_in_v2_index_completed) of
				false ->
					ar_storage:read_chunk(ChunkDataDBKey);
				true ->
					not_found
			end;
		{ok, Value} ->
			{ok, binary_to_term(Value)};
		Error ->
			Error
	end.

delete_chunk(ChunkDataKey, State) ->
	#state{
		chunk_data_db = ChunkDataDB
	} = State,
	case byte_size(ChunkDataKey) of
		32 ->
			ar_storage:delete_chunk(ChunkDataKey);
		64 ->
			ar_kv:delete(ChunkDataDB, ChunkDataKey)
	end.

init_kv() ->
	BaseOpts = [
		{max_open_files, 1000000}
	],
	BloomFilterOpts = [
		{block_based_table_options, [
			{cache_index_and_filter_blocks, true}, % Keep bloom filters in memory.
			{bloom_filter_policy, 10} % ~1% false positive probability.
		]},
		{optimize_filters_for_hits, true}
	],
	PrefixBloomFilterOpts =
		BloomFilterOpts ++ [
			{prefix_extractor, {capped_prefix_transform, ?OFFSET_KEY_PREFIX_BITSIZE div 8}}],
	ColumnFamilyDescriptors = [
		{"default", BaseOpts},
		{"chunks_index", BaseOpts ++ PrefixBloomFilterOpts},
		{"data_root_index", BaseOpts ++ BloomFilterOpts},
		{"data_root_offset_index", BaseOpts},
		{"tx_index", BaseOpts ++ BloomFilterOpts},
		{"tx_offset_index", BaseOpts},
		{"disk_pool_chunks_index", BaseOpts ++ BloomFilterOpts},
		{"migrations_index", BaseOpts}
	],
	{ok, DB, [_, CF1, CF2, CF3, CF4, CF5, CF6, CF7]} =
		ar_kv:open("ar_data_sync_db", ColumnFamilyDescriptors),
	{ok, ChunkDataDB} =
		ar_kv:open_without_column_families(
			"ar_data_sync_chunk_db", [
				{max_open_files, 1000000},
				{write_buffer_size, 256 * 1024 * 1024}, % 256 MiB per memtable.
				{target_file_size_base, 256 * 1024 * 1024}, % 256 MiB per SST file.
				%% 10 files in L1 to make L1 == L0 as recommended by the
				%% RocksDB guide https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide.
				{max_bytes_for_level_base, 10 * 256 * 1024 * 1024}
			]
		),
	State = #state{
		chunks_index = {DB, CF1},
		data_root_index = {DB, CF2},
		data_root_offset_index = {DB, CF3},
		tx_index = {DB, CF4},
		tx_offset_index = {DB, CF5},
		disk_pool_chunks_index = {DB, CF6},
		migrations_index = {DB, CF7},
		chunk_data_db = ChunkDataDB
	},
	ets:insert(?MODULE, [
		{chunks_index, {DB, CF1}},
		{data_root_index, {DB, CF2}},
		{data_root_offset_index, {DB, CF3}},
		{tx_index, {DB, CF4}},
		{tx_offset_index, {DB, CF5}},
		{disk_pool_chunks_index, {DB, CF6}},
		{chunk_data_db, ChunkDataDB}
	]),
	State.

read_data_sync_state() ->
	case ar_storage:read_term(data_sync_state) of
		{ok, {SyncRecord, RecentBI, RawDiskPoolDataRoots, DiskPoolSize}} ->
			DiskPoolDataRoots = filter_invalid_data_roots(RawDiskPoolDataRoots),
			WeaveSize = case RecentBI of [] -> 0; _ -> element(2, hd(RecentBI)) end,
			{SyncRecord, RecentBI, DiskPoolDataRoots, DiskPoolSize, WeaveSize};
		not_found ->
			{ar_intervals:new(), [], #{}, 0, 0}
	end.

migrate_index(State) ->
	#state{
		migrations_index = MigrationsDB
	} = State,
	%% To add a migration, add a key to the migrations column family with a value
	%% representing the current migration's stage, and trigger the migration here.
	Key = <<"store_data_in_v2_index">>,
	case ar_kv:get(MigrationsDB, Key) of
		{ok, <<"complete">>} ->
			ets:insert(?MODULE, {store_data_in_v2_index_completed}),
			ok;
		not_found ->
			gen_server:cast(?MODULE, {migrate, Key, first});
		{ok, Cursor} ->
			gen_server:cast(?MODULE, {migrate, Key, {seek, Cursor}})
	end.

migrate_chunk(ChunkKey, ChunkValue, State) ->
	#state{
		chunks_index = ChunksIndex,
		chunk_data_db = ChunkDataDB,
		data_root_offset_index = DataRootOffsetIndex,
		data_root_index = DataRootIndex,
		sync_record = SyncRecord
	} = State,
	ChunkIndexData = binary_to_term(ChunkValue),
	{DataPathHash, TXRoot, DataRoot, TXPath, ChunkOffset, ChunkSize} = ChunkIndexData,
	<< AbsoluteEndOffset:?OFFSET_KEY_BITSIZE >> = ChunkKey,
	case byte_size(DataPathHash) of
		64 ->
			{ok, State};
		32 ->
			case ar_storage:read_chunk(DataPathHash) of
				not_found ->
					case ar_kv:delete(ChunksIndex, ChunkKey) of
						ok ->
							ar_ets_intervals:delete(
								data_sync_record,
								AbsoluteEndOffset,
								AbsoluteEndOffset - ChunkSize
							),
							SyncRecord2 =
								ar_intervals:delete(
									SyncRecord,
									AbsoluteEndOffset,
									AbsoluteEndOffset - ChunkSize
								),
							{ok, State#state{ sync_record = SyncRecord2 }};
						Error ->
							Error
					end;
				{ok, ChunkData} ->
					ChunkDataValue = term_to_binary(ChunkData),
					ChunkDataDBKey = generate_chunk_data_db_key(DataPathHash),
					ChunkIndexData2 =
						term_to_binary({
							ChunkDataDBKey,
							element(2, ChunkIndexData),
							element(3, ChunkIndexData),
							element(4, ChunkIndexData),
							element(5, ChunkIndexData),
							element(6, ChunkIndexData)
						}),
					case ar_kv:put(ChunkDataDB, ChunkDataDBKey, ChunkDataValue) of
						ok ->
							{ok, _, DataRootOffsetIndexValue} =
								ar_kv:get_prev(
									DataRootOffsetIndex,
									<< (AbsoluteEndOffset - 1):?OFFSET_KEY_BITSIZE >>
								),
							{_, BlockSize, _} = binary_to_term(DataRootOffsetIndexValue),
							TXOffset = ar_merkle:extract_note(TXPath) - 1,
							{DataRoot, TXStartOffset, TXEndOffset} =
								ar_merkle:validate_path(TXRoot, TXOffset, BlockSize, TXPath),
							TXSize = TXEndOffset - TXStartOffset,
							DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
							case ar_kv:get(DataRootIndex, DataRootKey) of
								not_found ->
									case ar_kv:put(ChunksIndex, ChunkKey, ChunkIndexData2) of
										ok ->
											ar_storage:delete_chunk(DataPathHash),
											{ok, State};
										Error ->
											Error
									end;
								{ok, DataRootIndexValue} ->
									DataRootMap = binary_to_term(DataRootIndexValue),
									case migrate_chunks_index_chunks(
										ChunkDataDBKey,
										ChunkOffset,
										data_root_index_iterator(DataRootMap),
										State
									) of
										ok ->
											ar_storage:delete_chunk(DataPathHash),
											{ok, State};
										Error ->
											Error
									end;
								Error ->
									Error
							end;
						Error ->
							Error
					end;
				Error ->
					Error
			end
	end.

migrate_chunks_index_chunks(ChunkDataDBKey, ChunkOffset, Iterator, State) ->
	#state{
		chunks_index = ChunksIndex
	} = State,
	case next(Iterator) of
		none ->
			ok;
		{{_, TXStartOffset, _}, Iterator2} ->
			AbsoluteEndOffset = TXStartOffset + ChunkOffset,
			ChunkKey = << AbsoluteEndOffset:?OFFSET_KEY_BITSIZE >>,
			case ar_kv:get(ChunksIndex, ChunkKey) of
				not_found ->
					migrate_chunks_index_chunks(
						ChunkDataDBKey,
						ChunkOffset,
						Iterator2,
						State
					);
				{ok, Value} ->
					{_, TXRoot, DataRoot, TXPath, ChunkOffset, ChunkSize} =
						binary_to_term(Value),
					ChunkIndexData =
						term_to_binary(
							{ChunkDataDBKey, TXRoot, DataRoot, TXPath, ChunkOffset, ChunkSize}
						),
					case ar_kv:put(ChunksIndex, ChunkKey, ChunkIndexData) of
						ok ->
							migrate_chunks_index_chunks(
								ChunkDataDBKey,
								ChunkOffset,
								Iterator2,
								State
							);
						Error ->
							Error
					end;
				Error ->
					Error
			end
	end.

filter_invalid_data_roots(DiskPoolDataRoots) ->
	%% Filter out the keys with the invalid values, if any,
	%% produced by a bug in 2.1.0.0.
	maps:filter(
		fun (_, {_, _, _}) ->
				true;
			(_, _) ->
				false
		end,
		DiskPoolDataRoots
	).

data_root_offset_index_from_block_index(Index, BI, StartOffset) ->
	data_root_offset_index_from_reversed_block_index(Index, lists:reverse(BI), StartOffset).

data_root_offset_index_from_reversed_block_index(
	Index,
	[{_, Offset, _} | BI],
	Offset
) ->
	data_root_offset_index_from_reversed_block_index(Index, BI, Offset);
data_root_offset_index_from_reversed_block_index(
	Index,
	[{_, WeaveSize, TXRoot} | BI],
	StartOffset
) ->
	case ar_kv:put(
		Index,
		<< StartOffset:?OFFSET_KEY_BITSIZE >>,
		term_to_binary({TXRoot, WeaveSize - StartOffset, sets:new()})
	) of
		ok ->
			data_root_offset_index_from_reversed_block_index(Index, BI, WeaveSize);
		Error ->
			Error
	end;
data_root_offset_index_from_reversed_block_index(_Index, [], _StartOffset) ->
	ok.

remove_orphaned_data(State, BlockStartOffset, WeaveSize) ->
	ok = remove_orphaned_txs(State, BlockStartOffset, WeaveSize),
	{ok, OrphanedDataRoots} = remove_orphaned_data_roots(State, BlockStartOffset),
	ok = remove_orphaned_data_root_offsets(State, BlockStartOffset, WeaveSize),
	ok = remove_orphaned_chunks(State, BlockStartOffset, WeaveSize),
	{ok, OrphanedDataRoots}.

remove_orphaned_txs(State, BlockStartOffset, WeaveSize) ->
	#state{
		tx_offset_index = TXOffsetIndex,
		tx_index = TXIndex
	} = State,
	ok = case ar_kv:get_range(TXOffsetIndex, << BlockStartOffset:?OFFSET_KEY_BITSIZE >>) of
		{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
			ok;
		{ok, Map} ->
			maps:fold(
				fun
					(_, _Value, {error, _} = Error) ->
						Error;
					(_, TXID, ok) ->
						ar_kv:delete(TXIndex, TXID),
						ar_tx_blacklist:norify_about_orphaned_tx(TXID)
				end,
				ok,
				Map
			);
		Error ->
			Error
	end,
	ar_kv:delete_range(
		TXOffsetIndex,
		<< BlockStartOffset:?OFFSET_KEY_BITSIZE >>,
		<< (WeaveSize + 1):?OFFSET_KEY_BITSIZE >>
	).

remove_orphaned_chunks(State, BlockStartOffset, WeaveSize) ->
	#state{ chunks_index = ChunksIndex } = State,
	EndKey = << (WeaveSize + 1):?OFFSET_KEY_BITSIZE >>,
	ar_kv:delete_range(ChunksIndex, << (BlockStartOffset + 1):?OFFSET_KEY_BITSIZE >>, EndKey).

remove_orphaned_data_roots(State, BlockStartOffset) ->
	#state{
		data_root_offset_index = DataRootOffsetIndex,
		data_root_index = DataRootIndex
	} = State,
	case ar_kv:get_range(DataRootOffsetIndex, << BlockStartOffset:?OFFSET_KEY_BITSIZE >>) of
		{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
			{ok, sets:new()};
		{ok, Map} ->
			maps:fold(
				fun
					(_, _Value, {error, _} = Error) ->
						Error;
					(_, Value, {ok, OrphanedDataRoots}) ->
						{TXRoot, _BlockSize, DataRootIndexKeySet} = binary_to_term(Value),
						sets:fold(
							fun (_Key, {error, _} = Error) ->
									Error;
								(Key, {ok, Orphaned}) ->
									case remove_orphaned_data_root(
										DataRootIndex,
										Key,
										TXRoot,
										BlockStartOffset
									) of
										removed ->
											{ok, sets:add_element(Key, Orphaned)};
										ok ->
											{ok, Orphaned};
										Error ->
											Error
									end
							end,
							{ok, OrphanedDataRoots},
							DataRootIndexKeySet
						)
				end,
				{ok, sets:new()},
				Map
			);
		Error ->
			Error
	end.

remove_orphaned_data_root(DataRootIndex, DataRootKey, TXRoot, StartOffset) ->
	case ar_kv:get(DataRootIndex, DataRootKey) of
		not_found ->
			ok;
		{ok, Value} ->
			TXRootMap = binary_to_term(Value),
			case maps:is_key(TXRoot, TXRootMap) of
				false ->
					ok;
				true ->
					OffsetMap = maps:get(TXRoot, TXRootMap),
					UpdatedTXRootMap = case maps:filter(
						fun(TXStartOffset, _) ->
							TXStartOffset < StartOffset
						end,
						OffsetMap
					) of
						EmptyMap when map_size(EmptyMap) == 0 ->
							maps:remove(TXRoot, TXRootMap);
						UpdatedOffsetMap ->
							maps:put(TXRoot, UpdatedOffsetMap, TXRootMap)
					end,
					case UpdatedTXRootMap of
						EmptyTXRootMap when map_size(EmptyTXRootMap) == 0 ->
							case ar_kv:delete(DataRootIndex, DataRootKey) of
								ok ->
									removed;
								Error ->
									Error
							end;
						_ ->
							ar_kv:put(
								DataRootIndex,
								DataRootKey,
								term_to_binary(UpdatedTXRootMap)
							)
					end
			end
	end.

remove_orphaned_data_root_offsets(State, BlockStartOffset, WeaveSize) ->
	#state{
		data_root_offset_index = DataRootOffsetIndex
	} = State,
	ar_kv:delete_range(
		DataRootOffsetIndex,
		<< BlockStartOffset:?OFFSET_KEY_BITSIZE >>,
		<< (WeaveSize + 1):?OFFSET_KEY_BITSIZE >>
	).

have_free_space() ->
	ar_storage:get_free_space() > ?DISK_DATA_BUFFER_SIZE.

add_block(B, SizeTaggedTXs, State) ->
	#state{
		tx_index = TXIndex,
		tx_offset_index = TXOffsetIndex
	} = State,
	BlockStartOffset = B#block.weave_size - B#block.block_size,
	{ok, _} = add_block_data_roots(State, SizeTaggedTXs, BlockStartOffset),
	ok = update_tx_index(TXIndex, TXOffsetIndex, SizeTaggedTXs, BlockStartOffset).

update_tx_index(_TXIndex, _TXOffsetIndex, [], _BlockStartOffset) ->
	ok;
update_tx_index(TXIndex, TXOffsetIndex, SizeTaggedTXs, BlockStartOffset) ->
	lists:foldl(
		fun ({_, Offset}, Offset) ->
				Offset;
			({{TXID, _}, TXEndOffset}, PreviousOffset) ->
				AbsoluteEndOffset = BlockStartOffset + TXEndOffset,
				TXSize = TXEndOffset - PreviousOffset,
				AbsoluteStartOffset = AbsoluteEndOffset - TXSize,
				case ar_kv:put(
					TXOffsetIndex,
					<< AbsoluteStartOffset:?OFFSET_KEY_BITSIZE >>,
					TXID
				) of
					ok ->
						case ar_kv:put(
							TXIndex,
							TXID,
							term_to_binary({AbsoluteEndOffset, TXSize})
						) of
							ok ->
								ar_tx_blacklist:notify_about_added_tx(
									TXID,
									AbsoluteEndOffset,
									AbsoluteStartOffset
								),
								TXEndOffset;
							{error, Reason} ->
								?LOG_ERROR([{event, failed_to_update_tx_index}, {reason, Reason}]),
								TXEndOffset
						end;
					{error, Reason} ->
						?LOG_ERROR([{event, failed_to_update_tx_offset_index}, {reason, Reason}]),
						TXEndOffset
				end
		end,
		0,
		SizeTaggedTXs
	),
	ok.

add_block_data_roots(_State, [], _CurrentWeaveSize) ->
	{ok, sets:new()};
add_block_data_roots(State, SizeTaggedTXs, CurrentWeaveSize) ->
	#state{
		data_root_offset_index = DataRootOffsetIndex
	} = State,
	SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
	{TXRoot, TXTree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
	{BlockSize, DataRootIndexKeySet} = lists:foldl(
		fun ({_DataRoot, Offset}, {Offset, _} = Acc) ->
				Acc;
			({DataRoot, TXEndOffset}, {TXStartOffset, CurrentDataRootSet}) ->
				TXPath = ar_merkle:generate_path(TXRoot, TXEndOffset - 1, TXTree),
				TXOffset = CurrentWeaveSize + TXStartOffset,
				TXSize = TXEndOffset - TXStartOffset,
				DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
				ok = update_data_root_index(
					State,
					DataRootKey,
					TXRoot,
					TXOffset,
					TXPath
				),
				{TXEndOffset, sets:add_element(DataRootKey, CurrentDataRootSet)}
		end,
		{0, sets:new()},
		SizeTaggedDataRoots
	),
	case BlockSize > 0 of
		true ->
			ok = ar_kv:put(
				DataRootOffsetIndex,
				<< CurrentWeaveSize:?OFFSET_KEY_BITSIZE >>,
				term_to_binary({TXRoot, BlockSize, DataRootIndexKeySet})
			);
		false ->
			do_not_update_data_root_offset_index
	end,
	{ok, DataRootIndexKeySet}.

update_data_root_index(State, DataRootKey, TXRoot, AbsoluteTXStartOffset, TXPath) ->
	#state{
		data_root_index = DataRootIndex
	} = State,
	TXRootMap = case ar_kv:get(DataRootIndex, DataRootKey) of
		not_found ->
			#{};
		{ok, Value} ->
			binary_to_term(Value)
	end,
	OffsetMap = maps:get(TXRoot, TXRootMap, #{}),
	case maps:is_key(AbsoluteTXStartOffset, OffsetMap) of
		true ->
			ok;
		false ->
			UpdatedValue = term_to_binary(
				TXRootMap#{ TXRoot => OffsetMap#{ AbsoluteTXStartOffset => TXPath } }
			),
			ar_kv:put(DataRootIndex, DataRootKey, UpdatedValue)
	end.

add_block_data_roots_to_disk_pool(DataRoots, DataRootIndexKeySet) ->
	{U, _} = sets:fold(
		fun(R, {M, T}) ->
			{maps:update_with(
				R,
				fun({_, Timeout, _}) -> {0, Timeout, not_set} end,
				{0, T, not_set},
				M
			), T + 1}
		end,
		{DataRoots, os:system_time(microsecond)},
		DataRootIndexKeySet
	),
	U.

reset_orphaned_data_roots_disk_pool_timestamps(DataRoots, DataRootIndexKeySet) ->
	{U, _} = sets:fold(
		fun(R, {M, T}) ->
			{maps:update_with(
				R,
				fun({Size, _, TXIDSet}) -> {Size, T, TXIDSet} end,
				{0, T, not_set},
				M
			), T + 1}
		end,
		{DataRoots, os:system_time(microsecond)},
		DataRootIndexKeySet
	),
	U.

store_sync_state(State) ->
	#state{
		sync_record = SyncRecord,
		disk_pool_data_roots = DiskPoolDataRoots,
		disk_pool_size = DiskPoolSize,
		block_index = BI
	} = State,
	record_v2_index_data_size(State),
	ar_metrics:store(disk_pool_chunks_count),
	ar_storage:write_term(data_sync_state, {SyncRecord, BI, DiskPoolDataRoots, DiskPoolSize}).

record_v2_index_data_size(State) ->
	#state{ sync_record = SyncRecord } = State,
	prometheus_gauge:set(v2_index_data_size, ar_intervals:sum(SyncRecord)).

get_random_interval(SyncRecord, PeerSyncRecords, WeaveSize) ->
	%% Try keeping no more than ?MAX_SHARED_SYNCED_INTERVALS_COUNT intervals
	%% in the sync record by choosing the appropriate size of continuous
	%% intervals to sync. The motivation is to keep the record size small for
	%% low traffic overhead. When the size increases ?MAX_SHARED_SYNCED_INTERVALS_COUNT,
	%% a random subset of the intervals is served to peers.
	%% Include at least one byte - relevant for tests where the weave can be very small.
	SyncSize = max(1, WeaveSize div ?MAX_SHARED_SYNCED_INTERVALS_COUNT),
	lists:foldl(
		fun ({_PeerID, _PeerSyncRecord}, Acc) when length(Acc) > 5 ->
				Acc;
			({PeerID, PeerSyncRecord}, Acc) ->
				PeerSyncRecordBelowWeaveSize = ar_intervals:cut(PeerSyncRecord, WeaveSize),
				I = ar_intervals:outerjoin(SyncRecord, PeerSyncRecordBelowWeaveSize),
				Sum = ar_intervals:sum(I),
				case Sum of
					0 ->
						Acc;
					_ ->
						RelativeByte = rand:uniform(Sum) - 1,
						{L, Byte, R} =
							ar_intervals:get_interval_by_nth_inner_number(I, RelativeByte),
						LeftBound = max(L, Byte - SyncSize div 2),
						[{PeerID, LeftBound, min(R, LeftBound + SyncSize)} | Acc]
				end
		end,
		[],
		PeerSyncRecords
	).

validate_proof(_, _, _, _, Chunk, _) when byte_size(Chunk) > ?DATA_CHUNK_SIZE ->
	false;
validate_proof(TXRoot, TXPath, DataPath, Offset, Chunk, BlockSize) ->
	case ar_merkle:validate_path(TXRoot, Offset, BlockSize, TXPath) of
		false ->
			false;
		{DataRoot, TXStartOffset, TXEndOffset} ->
			ChunkOffset = Offset - TXStartOffset,
			TXSize = TXEndOffset - TXStartOffset,
			case ar_merkle:validate_path(DataRoot, ChunkOffset, TXSize, DataPath) of
				false ->
					false;
				{ChunkID, ChunkStartOffset, ChunkEndOffset} ->
					case ar_tx:generate_chunk_id(Chunk) == ChunkID of
						false ->
							false;
						true ->
							case ChunkEndOffset - ChunkStartOffset == byte_size(Chunk) of
								true ->
									{true, DataRoot, TXStartOffset, ChunkEndOffset, TXSize};
								false ->
									false
							end
					end
			end
	end.

validate_data_path(_, _, _, _, Chunk) when byte_size(Chunk) > ?DATA_CHUNK_SIZE ->
	false;
validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) ->
	case ar_merkle:validate_path(DataRoot, Offset, TXSize, DataPath) of
		false ->
			false;
		{ChunkID, StartOffset, EndOffset} ->
			case ar_tx:generate_chunk_id(Chunk) == ChunkID of
				false ->
					false;
				true ->
					case EndOffset - StartOffset == byte_size(Chunk) of
						true ->
							{true, EndOffset};
						false ->
							false
					end
			end
	end.

add_chunk(DataRoot, DataPath, Chunk, Offset, TXSize, State) ->
	#state{
		data_root_index = DataRootIndex,
		disk_pool_data_roots = DiskPoolDataRoots,
		disk_pool_chunks_index = DiskPoolChunksIndex,
		disk_pool_size = DiskPoolSize,
		chunk_data_db = ChunkDataDB
	} = State,
	DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	InDataRootIndex = ar_kv:get(DataRootIndex, DataRootKey),
	DataRootInDiskPool = maps:is_key(DataRootKey, DiskPoolDataRoots),
	case {InDataRootIndex, DataRootInDiskPool} of
		{not_found, false} ->
			{{error, data_root_not_found}, State};
		{not_found, true} ->
			{Size, Timestamp, TXIDSet} = maps:get(DataRootKey, DiskPoolDataRoots),
			DataRootLimit = ar_meta_db:get(max_disk_pool_data_root_buffer_mb) * 1024 * 1024,
			DiskPoolLimit = ar_meta_db:get(max_disk_pool_buffer_mb) * 1024 * 1024,
			ChunkSize = byte_size(Chunk),
			case Size + ChunkSize > DataRootLimit
					orelse DiskPoolSize + ChunkSize > DiskPoolLimit of
				true ->
					{{error, exceeds_disk_pool_size_limit}, State};
				false ->
					case validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) of
						false ->
							{{error, invalid_proof}, State};
						{true, EndOffset} ->
							DataPathHash = crypto:hash(sha256, DataPath),
							DiskPoolChunkKey = << Timestamp:256, DataPathHash/binary >>,
							case ar_kv:get(DiskPoolChunksIndex, DiskPoolChunkKey) of
								{ok, _DiskPoolChunk} ->
									%% The chunk is already in disk pool.
									{ok, State};
								not_found ->
									ChunkDataDBKey =
										generate_chunk_data_db_key(DataPathHash),
									ok = ar_kv:put(
										ChunkDataDB,
										ChunkDataDBKey,
										term_to_binary({Chunk, DataPath})
									),
									DiskPoolChunkValue =
										term_to_binary({
											EndOffset,
											ChunkSize,
											DataRoot,
											TXSize,
											ChunkDataDBKey
										}),
									ok = ar_kv:put(
										DiskPoolChunksIndex,
										DiskPoolChunkKey,
										DiskPoolChunkValue
									),
									prometheus_gauge:inc(disk_pool_chunks_count),
									DiskPoolDataRooots2 =
										maps:put(
											DataRootKey,
											{Size + ChunkSize, Timestamp, TXIDSet},
											DiskPoolDataRoots
										),
									State2 = State#state{
										disk_pool_size = DiskPoolSize + ChunkSize,
										disk_pool_data_roots = DiskPoolDataRooots2
									},
									{ok, State2}
							end
					end
			end;
		{{ok, Value}, _} ->
			DataRootMap = binary_to_term(Value),
			case validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) of
				false ->
					{{error, invalid_proof}, State};
				{true, EndOffset} ->
					store_chunk(
						{
							DataRootMap,
							DataRoot,
							EndOffset,
							TXSize,
							DataPath,
							Chunk
						},
						State
					)
			end
	end.

%% Register the chunk in the index under the corresponding global offsets and store its data
%% in the blob storage if it has not been stored yet. The function is called when a chunk
%% is pushed to the node or fetched from a peer. Every chunk is identified by the SHA2-256 hash
%% of the merkle proof of inclusion to the corresponding data tree (DataPath in the code).
%% The same data can be uploaded by different transactions, therefore the same chunk may be
%% registered under multiple global offsets. After the store_data_in_v2_index migration,
%% the blob storage keys are prefixed by timestamp to reduce the number of LSM compactions.
%% Consequently, in order to find out whether a new chunk already exists under some other
%% offset, we need to traverse the data root index - if the search is successful, we pick up
%% the timestamp-prefixed blob storage identifier of the chunk and register it under the new
%% global offset. If no chunk is found, we generate a timestamp and put the chunk into the
%% blob storage. Additionally, if the (data root, data size) pair is found in the disk pool,
%% the new chunk is placed to the disk pool so that it can survive chain reorganisations and
%% be picked up from the disk pool later. When the chunk is registered under the new global
%% offset, the sync record is updated. Returns {ok, State} | {{error, Reason}, State}.
store_chunk(Args, State) ->
	{
		DataRootMap,
		DataRoot,
		EndOffset,
		TXSize,
		DataPath,
		Chunk
	} = Args,
	#state{
		chunk_data_db = ChunkDataDB
	} = State,
	DataPathHash = crypto:hash(sha256, DataPath),
	{Status, ChunkDataKey} =
		find_or_generate_chunk_data_key(
			{
				DataPathHash,
				DataRoot,
				TXSize,
				EndOffset,
				data_root_index_iterator(DataRootMap)
			},
			State
		),
	MaybeStoreDataResult =
		case Status of
			generated ->
				ar_kv:put(ChunkDataDB, ChunkDataKey, term_to_binary({Chunk, DataPath}));
			found ->
				ok;
			found_in_disk_pool ->
				ok
		end,
	ChunkSize = byte_size(Chunk),
	MaybeAddChunkToDiskPoolResult =
		case MaybeStoreDataResult of
			ok ->
				case Status of
					found_in_disk_pool ->
						ok;
					_ ->
						maybe_add_chunk_to_disk_pool(
							{
								DataRoot,
								TXSize,
								DataPathHash,
								ChunkDataKey,
								EndOffset,
								ChunkSize
							},
							State
						)
				end;
			{error, Reason} = Error ->
				?LOG_ERROR([
					{event, failed_to_store_chunk},
					{reason, Reason}
				]),
				Error
		end,
	case MaybeAddChunkToDiskPoolResult of
		ok ->
			case mupdate_chunks_index(
				{
					data_root_index_iterator(DataRootMap),
					DataRoot,
					EndOffset,
					ChunkDataKey,
					ChunkSize
				},
				State
			) of
				{ok, State2} ->
					{ok, State2};
				Error2 ->
					Error2
			end;
		Error2 ->
			{Error2, State}
	end.

find_or_generate_chunk_data_key(Args, State) ->
	{
		DataPathHash,
		DataRoot,
		TXSize,
		Offset,
		Iterator
	} = Args,
	#state{
		sync_record = SyncRecord,
		chunks_index = ChunksIndex
	} = State,
	case next(Iterator) of
		none ->
			find_or_generate_chunk_data_key2(DataPathHash, DataRoot, TXSize, State);
		{{_, TXStartOffset, _}, Iterator2} ->
			AbsoluteEndOffset = TXStartOffset + Offset,
			case ar_intervals:is_inside(SyncRecord, AbsoluteEndOffset) of
				true ->
					case ar_kv:get(ChunksIndex, << AbsoluteEndOffset:?OFFSET_KEY_BITSIZE >>) of
						{ok, Value} ->
							{found, element(1, binary_to_term(Value))};
						_ ->
							find_or_generate_chunk_data_key(
								{
									DataPathHash,
									DataRoot,
									TXSize,
									Offset,
									Iterator2
								},
								State
							)
					end;
				false ->
					find_or_generate_chunk_data_key(
						{
							DataPathHash,
							DataRoot,
							TXSize,
							Offset,
							Iterator2
						},
						State
					)
			end
	end.

find_or_generate_chunk_data_key2(DataPathHash, DataRoot, TXSize, State) ->
	#state{
		disk_pool_data_roots = DiskPoolDataRoots,
		disk_pool_chunks_index = DiskPoolChunksIndex
	} = State,
	DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	case maps:get(DataRootKey, DiskPoolDataRoots, not_found) of
		not_found ->
			{generated, generate_chunk_data_db_key(DataPathHash)};
		{_, Timestamp, _} ->
			DiskPoolChunkKey = << Timestamp:256, DataPathHash/binary >>,
			case ar_kv:get(DiskPoolChunksIndex, DiskPoolChunkKey) of
				not_found ->
					{generated, generate_chunk_data_db_key(DataPathHash)};
				{ok, Value} ->
					DiskPoolChunkData = binary_to_term(Value),
					case size(DiskPoolChunkData) of
						5 ->
							{found_in_disk_pool, element(5, DiskPoolChunkData)};
						4 ->
							%% A chunk from before the store_data_in_v2_index migration.
							{found_in_disk_pool, DataPathHash}
					end
			end
	end.

generate_chunk_data_db_key(DataPathHash) ->
	Timestamp = os:system_time(microsecond),
	<< Timestamp:256, DataPathHash/binary >>.

%% Records the given chunk metadata under all of the chunk's global offsets.
mupdate_chunks_index(Args, State) ->
	{
		DataRootIndexIterator,
		DataRoot,
		EndOffset,
		ChunkDataKey,
		ChunkSize
	} = Args,
	case next(DataRootIndexIterator) of
		none ->
			{ok, State};
		{{TXRoot, TXStartOffset, TXPath}, DataRootIndexIterator2} ->
			AbsoluteEndOffset = TXStartOffset + EndOffset,
			case update_chunks_index(
				{
					AbsoluteEndOffset,
					EndOffset,
					ChunkDataKey,
					TXRoot,
					DataRoot,
					TXPath,
					ChunkSize
				},
				State
			) of
				{updated, SyncRecord} ->
					State2 =
						State#state{
							sync_record = SyncRecord
						},
					mupdate_chunks_index(
						{
							DataRootIndexIterator2,
							DataRoot,
							EndOffset,
							ChunkDataKey,
							ChunkSize
						},
						State2
					);
				not_updated ->
					mupdate_chunks_index(
						{
							DataRootIndexIterator2,
							DataRoot,
							EndOffset,
							ChunkDataKey,
							ChunkSize
						},
						State
					);
				Error ->
					{Error, State}
			end
	end.

update_chunks_index(Args, State) ->
	{
		AbsoluteChunkOffset,
		ChunkOffset,
		ChunkDataKey,
		TXRoot,
		DataRoot,
		TXPath,
		ChunkSize
	} = Args,
	#state{
		sync_record = SyncRecord
	} = State,
	case ar_intervals:is_inside(SyncRecord, AbsoluteChunkOffset) of
		true ->
			not_updated;
		false ->
			case ar_tx_blacklist:is_byte_blacklisted(AbsoluteChunkOffset) of
				true ->
					not_updated;
				false ->
					update_chunks_index2(
						AbsoluteChunkOffset,
						ChunkOffset,
						ChunkDataKey,
						TXRoot,
						DataRoot,
						TXPath,
						ChunkSize,
						State
					)
			end
	end.

update_chunks_index2(
	AbsoluteChunkOffset,
	ChunkOffset,
	ChunkDataDBKey,
	TXRoot,
	DataRoot,
	TXPath,
	ChunkSize,
	State
) ->
	#state{
		chunks_index = ChunksIndex,
		sync_record = SyncRecord
	} = State,
	Key = << AbsoluteChunkOffset:?OFFSET_KEY_BITSIZE >>,
	Value = {ChunkDataDBKey, TXRoot, DataRoot, TXPath, ChunkOffset, ChunkSize},
	case ar_kv:put(ChunksIndex, Key, term_to_binary(Value)) of
		ok ->
			StartOffset = AbsoluteChunkOffset - ChunkSize,
			SyncRecord2 = ar_intervals:add(SyncRecord, AbsoluteChunkOffset, StartOffset),
			ar_ets_intervals:add(data_sync_record, AbsoluteChunkOffset, StartOffset),
			{updated, SyncRecord2};
		{error, Reason} ->
			?LOG_ERROR([
				{event, failed_to_update_chunk_index},
				{reason, Reason},
				{chunk, ar_util:encode(ChunkDataDBKey)},
				{offset, AbsoluteChunkOffset}
			]),
			{error, Reason}
	end.

maybe_add_chunk_to_disk_pool(Args, State) ->
	{DataRoot, TXSize, _DataPathHash, _ChunkDataDBKey, _ChunkOffset, _ChunkSize} = Args,
	#state{
		disk_pool_data_roots = DiskPoolDataRoots
	} = State,
	DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	case maps:get(DataRootKey, DiskPoolDataRoots, not_found) of
		not_found ->
			ok;
		{_, Timestamp, _} ->
			add_chunk_to_disk_pool(Timestamp, Args, State)
	end.

add_chunk_to_disk_pool(Timestamp, Args, State) ->
	{DataRoot, TXSize, DataPathHash, ChunkDataKey, ChunkOffset, ChunkSize} = Args,
	#state{
		disk_pool_chunks_index = DiskPoolChunksIndex
	} = State,
	DiskPoolChunkKey = << Timestamp:256, DataPathHash/binary >>,
	case ar_kv:get(DiskPoolChunksIndex, DiskPoolChunkKey) of
		not_found ->
			Value = {ChunkOffset, ChunkSize, DataRoot, TXSize, ChunkDataKey},
			case ar_kv:put(DiskPoolChunksIndex, DiskPoolChunkKey, term_to_binary(Value)) of
					ok ->
						prometheus_gauge:inc(disk_pool_chunks_count),
						ok;
					Error ->
						Error
			end;
		_ ->
			ok
	end.

pick_missing_blocks([{H, WeaveSize, _} | CurrentBI], BlockTXPairs) ->
	{After, Before} = lists:splitwith(fun({BH, _}) -> BH /= H end, BlockTXPairs),
	case Before of
		[] ->
			pick_missing_blocks(CurrentBI, BlockTXPairs);
		_ ->
			{WeaveSize, lists:reverse(After)}
	end.

cast_after(Delay, Message) ->
	timer:apply_after(Delay, gen_server, cast, [self(), Message]).

store_fetched_chunk(Args, State) ->
	{
		DataRoot,
		DataRootKey,
		DataRootOffsetKey,
		DataRootIndexKeySet,
		AbsoluteTXStartOffset,
		TXPath,
		TXRoot,
		TXSize,
		BlockSize,
		DataPath,
		ChunkEndOffset,
		Chunk
	} = Args,
	#state{
		data_root_offset_index = DataRootOffsetIndex,
		data_root_index = DataRootIndex
	} = State,
	%% The data_root_offset_index and data_root_index are updated in case
	%% we have not synced the corresponding blocks yet (in ar_header_sync).
	DataRootMap =
		case sets:is_element(DataRootKey, DataRootIndexKeySet) of
			true ->
				{ok, DataRootIndexValue} = ar_kv:get(DataRootIndex, DataRootKey),
				DataRootMap2 = binary_to_term(DataRootIndexValue),
				TXRootMap = maps:get(TXRoot, DataRootMap2, #{}),
				case maps:is_key(AbsoluteTXStartOffset, TXRootMap) of
					false ->
						DataRootMap3 =
							maps:put(
								TXRoot,
								TXRootMap#{ AbsoluteTXStartOffset => TXPath },
								DataRootMap2
							),
						ok =
							ar_kv:put(
								DataRootIndex,
								DataRootKey,
								term_to_binary(DataRootMap3)
							),
						DataRootMap3;
					true ->
						DataRootMap2
				end;
			false ->
				DataRootMap2 = #{ TXRoot => #{ AbsoluteTXStartOffset => TXPath } },
				ok = ar_kv:put(
					DataRootIndex,
					DataRootKey,
					term_to_binary(DataRootMap2)
				),
				UpdatedValue =
					term_to_binary({
						TXRoot,
						BlockSize,
						sets:add_element(DataRootKey, DataRootIndexKeySet)
					}),
				ok = ar_kv:put(
					DataRootOffsetIndex,
					DataRootOffsetKey,
					UpdatedValue
				),
				DataRootMap2
		end,
	case
		%% Here we do not just store the chunk under the single offset we fetched it by,
		%% but check whether we already have chunks with the same proof under other
		%% offsets (the case when the same data is uploaded by multiple transactions).
		%% The main motivation is to have a single key for the chunk in the blob storage
		%% so that we can count references when removing a chunk from a particular offset.
		%% For example, a chunk can be removed for being blacklisted while the same
		%% data was uploaded in a whitelisted transaction - in this case the blacklisted
		%% offset is removed but the chunk is kept in the storage because it is referenced
		%% by the whitelisted transaction.
		store_chunk(
			{
				DataRootMap,
				DataRoot,
				ChunkEndOffset,
				TXSize,
				DataPath,
				Chunk
			},
			State
		) of
			{ok, State2} ->
				{noreply, State2};
			{error, Reason} ->
				?LOG_ERROR([
					{event, failed_to_store_fetched_chunk},
					{reason, Reason},
					{relative_chunk_offset, ChunkEndOffset},
					{tx_size, TXSize},
					{data_root, ar_util:encode(DataRoot)}
				]),
				{noreply, State}
	end.

process_disk_pool_item(State, Key, Value, NextCursor) ->
	#state{
		disk_pool_chunks_index = DiskPoolChunksIndex,
		disk_pool_data_roots = DiskPoolDataRoots,
		data_root_index = DataRootIndex
	} = State,
	prometheus_counter:inc(disk_pool_processed_chunks),
	<< Timestamp:256, DataPathHash/binary >> = Key,
	DiskPoolChunk = binary_to_term(Value),
	{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey} =
		case size(DiskPoolChunk) of
			4 ->
				{Offset2, Size2, DataRoot2, TXSize2} = DiskPoolChunk,
				{Offset2, Size2, DataRoot2, TXSize2, DataPathHash};
			5 ->
				DiskPoolChunk
		end,
	DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	InDataRootIndex =
		case ar_kv:get(DataRootIndex, DataRootKey) of
			not_found ->
				not_found;
			{ok, DataRootIndexValue} ->
				binary_to_term(DataRootIndexValue)
		end,
	InDiskPool = maps:is_key(DataRootKey, DiskPoolDataRoots),
	InChunksIndex =
		case InDataRootIndex of
			not_found ->
				false;
			DataRootMap2 ->
				has_synced_chunk(Offset, data_root_index_iterator(DataRootMap2), State)
		end,
	case {InDataRootIndex, InDiskPool, InChunksIndex} of
		{_, true, true} ->
			%% Increment the timestamp by one (microsecond), so that the new cursor is
			%% a prefix of the first key of the next data root. We want to quickly skip
			%% all chunks belonging to the same data root because the chunks are already
			%% in the index.
			{noreply, State#state{ disk_pool_cursor = NextCursor }};
		{not_found, true, _} ->
			%% Increment the timestamp by one (microsecond), so that the new cursor is
			%% a prefix of the first key of the next data root. We want to quickly skip
			%% all chunks belonging to the same data root because the data root is not
			%% yet on chain.
			{noreply, State#state{ disk_pool_cursor = {skip_timestamp, Timestamp} }};
		{not_found, false, _} ->
			%% The chunk never made it to the chain, the data root has expired in disk pool.
			ok = ar_kv:delete(DiskPoolChunksIndex, Key),
			prometheus_gauge:dec(disk_pool_chunks_count),
			ok = delete_chunk(ChunkDataKey, State),
			{noreply, State#state{ disk_pool_cursor = NextCursor }};
		{_, false, true} ->
			%% The data root has expired in disk pool and the chunk is already in the index.
			ok = ar_kv:delete(DiskPoolChunksIndex, Key),
			prometheus_gauge:dec(disk_pool_chunks_count),
			{noreply, State#state{ disk_pool_cursor = NextCursor }};
		{DataRootMap, false, false} ->
			%% The data root has expired in disk pool, but we can still record the chunk
			%% in the index.
			case move_chunk_from_disk_pool(
					{
						ChunkDataKey,
						Key,
						Offset,
						ChunkSize,
						TXSize,
						DataRoot,
						DataRootMap
					},
					State
				) of
					{ok, State2} ->
						ok = ar_kv:delete(DiskPoolChunksIndex, Key),
						prometheus_gauge:dec(disk_pool_chunks_count),
						{noreply, State2#state{ disk_pool_cursor = NextCursor }};
					{error, Reason} ->
						?LOG_ERROR([
							{event, failed_to_move_chunk_from_disk_pool_to_index},
							{disk_pool_key, ar_util:encode(Key)},
							{reason, Reason}
						]),
						{noreply, State#state{ disk_pool_cursor = NextCursor }}
			end;
		{DataRootMap, true, false} ->
			%% Record the chunk in the index.
			case move_chunk_from_disk_pool(
					{
						ChunkDataKey,
						Key,
						Offset,
						ChunkSize,
						TXSize,
						DataRoot,
						DataRootMap
					},
					State
				) of
					{ok, State2} ->
						{noreply, State2#state{ disk_pool_cursor = NextCursor }};
					{error, Reason} ->
						?LOG_ERROR([
							{event, failed_to_move_chunk_from_disk_pool_to_index},
							{disk_pool_key, ar_util:encode(Key)},
							{reason, Reason}
						]),
						{noreply, State#state{ disk_pool_cursor = NextCursor }}
			end
	end.

has_synced_chunk(Offset, DataRootIndexIterator, State) ->
	#state{
		sync_record = SyncRecord
	} = State,
	case next(DataRootIndexIterator) of
		none ->
			false;
		{{_, TXStartOffset, _}, DataRootIndexIterator2} ->
			AbsoluteEndOffset = TXStartOffset + Offset,
			case ar_intervals:is_inside(SyncRecord, AbsoluteEndOffset) of
				true ->
					true;
				false ->
					has_synced_chunk(Offset, DataRootIndexIterator2, State)
			end
	end.

move_chunk_from_disk_pool(Args, State) ->
	{
		ChunkDataKey,
		DiskPoolChunksIndexKey,
		Offset,
		ChunkSize,
		TXSize,
		DataRoot,
		DataRootMap
	} = Args,
	#state{
		chunk_data_db = ChunkDataDB,
		disk_pool_chunks_index = DiskPoolChunksIndex
	} = State,
	GetChunkDataKeyResult =
		case byte_size(ChunkDataKey) of
			64 ->
				{ok, ChunkDataKey};
			32 ->
				ChunkDataKey2 = generate_chunk_data_db_key(ChunkDataKey),
				case ar_storage:read_chunk(ChunkDataKey) of
					not_found ->
						{error, no_chunk_found_in_storage_for_disk_pool_key};
					{error, _} = Error ->
						Error;
					{ok, Chunk} ->
						case ar_kv:put(
							ChunkDataDB,
							ChunkDataKey2,
							term_to_binary(Chunk)
						) of
							{error, _} = Error ->
								Error;
							ok ->
								case ar_kv:put(
									DiskPoolChunksIndex,
									DiskPoolChunksIndexKey,
									term_to_binary({
										Offset,
										ChunkSize,
										DataRoot,
										TXSize,
										ChunkDataKey2
									})
								) of
									{error, _} = Error ->
										Error;
									ok ->
										delete_chunk(ChunkDataKey, State),
										{ok, ChunkDataKey2}
								end
						end
				end
		end,
	case GetChunkDataKeyResult of
		{error, _} = Error2 ->
			Error2;
		{ok, ChunkDataKey3} ->
			mupdate_chunks_index(
				{
					data_root_index_iterator(DataRootMap),
					DataRoot,
					Offset,
					ChunkDataKey3,
					ChunkSize
				},
				State
			)
	end.

get_absolute_chunk_offsets(EndOffset, DataRootIndexIterator, State) ->
	#state{
		sync_record = SyncRecord
	} = State,
	case next(DataRootIndexIterator) of
		none ->
			[];
		{{_, TXStartOffset, _}, DataRootIndexIterator2} ->
			AbsoluteEndOffset = TXStartOffset + EndOffset,
			case ar_intervals:is_inside(SyncRecord, AbsoluteEndOffset) of
				true ->
					[AbsoluteEndOffset |
						get_absolute_chunk_offsets(EndOffset, DataRootIndexIterator2, State)];
				false ->
					get_absolute_chunk_offsets(EndOffset, DataRootIndexIterator2, State)
			end
	end.

get_tx_data_from_chunks(ChunkDataDB, Offset, Size, Map) ->
	get_tx_data_from_chunks(ChunkDataDB, Offset, Size, Map, <<>>).

get_tx_data_from_chunks(_ChunkDataDB, _Offset, 0, _Map, Data) ->
	{ok, iolist_to_binary(Data)};
get_tx_data_from_chunks(ChunkDataDB, Offset, Size, Map, Data) ->
	case maps:get(<< Offset:?OFFSET_KEY_BITSIZE >>, Map, not_found) of
		not_found ->
			{error, not_found};
		Value ->
			{ChunkID, _, _, _, _, ChunkSize} = binary_to_term(Value),
			case read_chunk(ChunkDataDB, ChunkID) of
				not_found ->
					{error, not_found};
				{error, Reason} ->
					?LOG_ERROR([{event, failed_to_read_chunk_for_tx_data}, {reason, Reason}]),
					{error, not_found};
				{ok, {Chunk, _}} ->
					get_tx_data_from_chunks(
						ChunkDataDB,
						Offset - ChunkSize,
						Size - ChunkSize,
						Map,
						[Chunk | Data]
					)
			end
	end.

data_root_index_iterator(TXRootMap) ->
	{maps:iterator(TXRootMap), none}.

next({TXRootMapIterator, none}) ->
	case maps:next(TXRootMapIterator) of
		none ->
			none;
		{TXRoot, OffsetMap, UpdatedTXRootMapIterator} ->
			OffsetMapIterator = maps:iterator(OffsetMap),
			{Offset, TXPath, UpdatedOffsetMapIterator} = maps:next(OffsetMapIterator),
			UpdatedIterator = {UpdatedTXRootMapIterator, {TXRoot, UpdatedOffsetMapIterator}},
			{{TXRoot, Offset, TXPath}, UpdatedIterator}
	end;
next({TXRootMapIterator, {TXRoot, OffsetMapIterator}}) ->
	case maps:next(OffsetMapIterator) of
		none ->
			next({TXRootMapIterator, none});
		{Offset, TXPath, UpdatedOffsetMapIterator} ->
			UpdatedIterator = {TXRootMapIterator, {TXRoot, UpdatedOffsetMapIterator}},
			{{TXRoot, Offset, TXPath}, UpdatedIterator}
	end.
