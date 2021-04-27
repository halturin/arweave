-module(ar_header_sync).

-behaviour(gen_server).

-export([
	start_link/0,
	join/2,
	add_tip_block/2, add_block/1,
	request_tx_removal/1
]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_header_sync.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").

%%% This module syncs block and transaction headers and maintains a persisted record of synced
%%% headers. Headers are synced from latest to earliest. Includes a migration process that
%%% moves data to v2 index for blocks written prior to the 2.1 update.

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Update the tip after the node joins the network.
join(BI, Blocks) ->
	gen_server:cast(?MODULE, {join, BI, Blocks}).

%% @doc Add a new tip block to the index and storage, record the new recent block index.
add_tip_block(B, RecentBI) ->
	gen_server:cast(?MODULE, {add_tip_block, B, RecentBI}).

%% @doc Add a block to the index and storage.
add_block(B) ->
	gen_server:cast(?MODULE, {add_block, B}).

%% @doc Remove the given transaction.
request_tx_removal(TXID) ->
	gen_server:cast(?MODULE, {remove_tx, TXID}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	?LOG_INFO([{event, ar_header_sync_start}]),
	process_flag(trap_exit, true),
	{ok, DB} = ar_kv:open("ar_header_sync_db"),
	{SyncRecord, LastHeight, CurrentBI} =
		case ar_storage:read_term(header_sync_state) of
			not_found ->
				{ar_intervals:new(), -1, []};
			{ok, StoredState} ->
				StoredState
		end,
	gen_server:cast(self(), check_space_alarm),
	gen_server:cast(self(), check_space_process_item),
	{ok,
		#{
			db => DB,
			sync_record => SyncRecord,
			last_height => LastHeight,
			block_index => CurrentBI,
			queue => queue:new(),
			last_picked => LastHeight,
			cleanup_started => false
		}}.

handle_cast({join, BI, Blocks}, State) ->
	#{
		db := DB,
		last_height := LastHeight,
		block_index := CurrentBI,
		sync_record := SyncRecord
	} = State,
	LastHeight2 = length(BI) - 1,
	State2 =
		State#{
			last_height => LastHeight2,
			block_index => lists:sublist(BI, ?HEADER_SYNC_TRACK_CONFIRMATIONS),
			last_picked => LastHeight2
		},
	State3 =
		case {CurrentBI, ar_util:get_block_index_intersection(BI, CurrentBI)} of
			{[], none} ->
				State2;
			{_CurrentBI, none} ->
				throw(last_stored_block_index_has_no_intersection_with_the_new_one);
			{_CurrentBI, {_Entry, Height}} ->
				S = State2#{ sync_record => ar_intervals:cut(SyncRecord, Height) },
				store_sync_state(S),
				%% Delete from the kv store only after the sync record is saved - no matter
				%% what happens to the process, if a height is in the record, it must be present
				%% in the kv store.
				ok = ar_kv:delete_range(DB, << (Height + 1):256 >>, << (LastHeight + 1):256 >>),
				S
		end,
	State4 =
		lists:foldl(
			fun(B, S) ->
				ar_data_sync:add_block(B, B#block.size_tagged_txs),
				add_block(B, S)
			end,
			State3,
			Blocks
		),
	store_sync_state(State4),
	{noreply, State4};

handle_cast({add_tip_block, #block{ height = Height } = B, RecentBI}, State) ->
	#{
		db := DB,
		sync_record := SyncRecord,
		block_index := CurrentBI,
		last_height := CurrentHeight
	} = State,
	BaseHeight = get_base_height(CurrentBI, CurrentHeight, RecentBI),
	State2 = State#{
		sync_record => ar_intervals:cut(SyncRecord, BaseHeight),
		block_index => RecentBI,
		last_height => Height
	},
	State3 = add_block(B, State2),
	store_sync_state(State3),
	%% Delete from the kv store only after the sync record is saved - no matter
	%% what happens to the process, if a height is in the record, it must be present
	%% in the kv store.
	ok = ar_kv:delete_range(DB, << (BaseHeight + 1):256 >>, << (CurrentHeight + 1):256 >>),
	{noreply, State3};

handle_cast({add_block, B}, State) ->
	State2 = add_block(B, State),
	store_sync_state(State2),
	{noreply, State2};

handle_cast(check_space_alarm, State) ->
	FreeSpace = ar_storage:get_free_space(),
	case FreeSpace < ?DISK_HEADERS_BUFFER_SIZE of
		true ->
			Msg =
				"The node has stopped syncing headers - the available disk space is"
				" less than ~s. Add more disk space if you wish to store more data."
				" When it is less than ~s, the node will remove some of the old block"
				" and transaction headers, consider adding some disk space.",
			?LOG_INFO(Msg, [
				ar_util:bytes_to_mb_string(?DISK_HEADERS_BUFFER_SIZE),
				ar_util:bytes_to_mb_string(?DISK_HEADERS_CLEANUP_THRESHOLD)
			]);
		false ->
			ok
	end,
	cast_after(ar_disksup:get_disk_space_check_frequency(), check_space_alarm),
	{noreply, State};

handle_cast(check_space_process_item, #{ cleanup_started := CleanupStarted } = State) ->
	FreeSpace = ar_storage:get_free_space(),
	case FreeSpace > ?DISK_HEADERS_BUFFER_SIZE of
		true ->
			gen_server:cast(self(), process_item),
			{noreply, State#{ cleanup_started => false }};
		false ->
			case FreeSpace < ?DISK_HEADERS_CLEANUP_THRESHOLD of
				true ->
					case CleanupStarted of
						true ->
							ok;
						false ->
							?LOG_INFO(
								"Removing older block and transaction headers to free up"
								" space for the new headers."
							)
					end,
					gen_server:cast(?MODULE, check_space_process_item),
					{noreply, remove_oldest_headers(State#{ cleanup_started => true })};
				false ->
					cast_after(?CHECK_AFTER_SYNCED_INTERVAL_MS, check_space_process_item),
					{noreply, State#{ cleanup_started => false }}
			end
	end;

handle_cast(process_item, State) ->
	#{
		queue := Queue,
		sync_record := SyncRecord,
		last_picked := LastPicked,
		last_height := LastHeight
	} = State,
	prometheus_gauge:set(downloader_queue_size, queue:len(Queue)),
	UpdatedQueue = process_item(Queue),
	case pick_unsynced_block(LastPicked, SyncRecord) of
		nothing_to_sync ->
			cast_after(?CHECK_AFTER_SYNCED_INTERVAL_MS, check_space_process_item),
			LastPicked2 =
				case queue:is_empty(UpdatedQueue) of
					true ->
						LastHeight;
					false ->
						LastPicked
				end,
			{noreply, State#{ queue => UpdatedQueue, last_picked => LastPicked2 }};
		Height ->
			cast_after(?PROCESS_ITEM_INTERVAL_MS, check_space_process_item),
			case ar_node:get_block_index_entry(Height) of
				true ->
					{noreply, State#{ queue => UpdatedQueue }};
				not_joined ->
					{noreply, State#{ queue => UpdatedQueue }};
				not_found ->
					?LOG_ERROR([
						{event, ar_header_sync_block_index_entry_not_found},
						{height, Height},
						{sync_record, SyncRecord}
					]),
					{noreply, State#{ queue => UpdatedQueue }};
				{H, _WeaveSize, TXRoot} ->
					%% Before 2.0, to compute a block hash, the complete wallet list
					%% and all the preceding hashes were required. Getting a wallet list
					%% and a hash list for every historical block to verify it belongs to
					%% the weave is very costly. Therefore, a list of 2.0 hashes for 1.0
					%% blocks was computed and stored along with the network client.
					H2 =
						case Height < ar_fork:height_2_0() of
							true ->
								ar_node:get_2_0_hash_of_1_0_block(Height);
							false ->
								not_set
						end,
					{noreply, State#{
						queue => enqueue({block, {H, H2, TXRoot}}, UpdatedQueue),
						last_picked => Height
					}}
			end
	end;

handle_cast({remove_tx, TXID}, State) ->
	{ok, _Size} = ar_storage:delete_tx(TXID),
	ar_tx_blacklist:notify_about_removed_tx(TXID),
	{noreply, State};

handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, State}.

handle_call(_Msg, _From, State) ->
	{reply, not_implemented, State}.

handle_info({_Ref, _Atom}, State) ->
	%% Some older versions of Erlang OTP have a bug where gen_tcp:close may leak
	%% a message. https://github.com/ninenines/gun/issues/193,
	%% https://bugs.erlang.org/browse/ERL-1049.
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {module, ?MODULE}, {message, Info}]),
	{noreply, State}.

terminate(Reason, State) ->
	?LOG_INFO([{event, ar_header_sync_terminate}, {reason, Reason}]),
	#{ db := DB } = State,
	ar_kv:close(DB).

%%%===================================================================
%%% Private functions.
%%%===================================================================

store_sync_state(State) ->
	#{ sync_record := SyncRecord, last_height := LastHeight, block_index := BI } = State,
	prometheus_gauge:set(synced_blocks, ar_intervals:sum(SyncRecord)),
	ok = ar_storage:write_term(header_sync_state, {SyncRecord, LastHeight, BI}).

get_base_height([{H, _, _} | CurrentBI], CurrentHeight, RecentBI) ->
	case lists:search(fun({BH, _, _}) -> BH == H end, RecentBI) of
		false ->
			get_base_height(CurrentBI, CurrentHeight - 1, RecentBI);
		_ ->
			CurrentHeight
	end.

add_block(B, State) ->
	#{ db := DB, sync_record := SyncRecord } = State,
	#block{ indep_hash = H, previous_block = PrevH, height = Height } = B,
	TXs = [TX || TX <- B#block.txs, not ar_tx_blacklist:is_tx_blacklisted(TX#tx.id)],
	case ar_storage:write_full_block(B, TXs) of
		ok ->
			case ar_intervals:is_inside(SyncRecord, Height) of
				true ->
					State;
				false ->
					ok = ar_kv:put(DB, << Height:256 >>, term_to_binary({H, PrevH})),
					UpdatedSyncRecord = ar_intervals:add(SyncRecord, Height, Height - 1),
					State#{ sync_record => UpdatedSyncRecord }
			end;
		{error, Reason} ->
			?LOG_WARNING([
				{event, failed_to_store_block},
				{block, ar_util:encode(H)},
				{height, Height},
				{reason, Reason}
			]),
			State
	end.

remove_oldest_headers(#{ db := DB, sync_record := SyncRecord } = State) ->
	case ar_intervals:count(SyncRecord) == 0 of
		true ->
			State;
		false ->
			{{_, Height}, _} = ar_intervals:take_smallest(SyncRecord),
			Height2 = Height + 1,
			case ar_kv:get(DB, << Height2:256 >>) of
				not_found ->
					State#{
						sync_record => ar_intervals:delete(SyncRecord, Height2, Height2 - 1)
					};
				{ok, Value} ->
					BH = element(1, binary_to_term(Value)),
					{ok, _BytesRemoved} = ar_storage:delete_full_block(BH),
					SyncRecord2 = ar_intervals:delete(SyncRecord, Height2, Height2 - 1),
					ok = ar_kv:delete(DB, << Height2:256 >>),
					State#{ sync_record => SyncRecord2 }
			end
	end.

cast_after(Delay, Message) ->
	timer:apply_after(Delay, gen_server, cast, [self(), Message]).

%% @doc Pick the biggest height smaller than LastPicked from outside the sync record.
pick_unsynced_block(LastPicked, SyncRecord) ->
	case ar_intervals:is_empty(SyncRecord) of
		true ->
			case LastPicked - 1 >= 0 of
				true ->
					LastPicked - 1;
				false ->
					nothing_to_sync
			end;
		false ->
			case ar_intervals:take_largest(SyncRecord) of
				{{_End, -1}, _SyncRecord2} ->
					nothing_to_sync;
				{{_End, Start}, SyncRecord2} when Start >= LastPicked ->
					pick_unsynced_block(LastPicked, SyncRecord2);
				{{End, _Start}, _SyncRecord2} when LastPicked - 1 > End ->
					LastPicked - 1;
				{{_End, Start}, _SyncRecord2} ->
					Start
			end
	end.

enqueue(Item, Queue) ->
	queue:in({Item, initial_backoff()}, Queue).

initial_backoff() ->
	{os:system_time(seconds), ?INITIAL_BACKOFF_INTERVAL_S}.

process_item(Queue) ->
	Now = os:system_time(second),
	case queue:out(Queue) of
		{empty, _Queue} ->
			Queue;
		{{value, {Item, {BackoffTimestamp, _} = Backoff}}, UpdatedQueue}
				when BackoffTimestamp > Now ->
			enqueue(Item, Backoff, UpdatedQueue);
		{{value, {{block, {H, H2, TXRoot}}, Backoff}}, UpdatedQueue} ->
			case read_block(H, H2, TXRoot) of
				error ->
					UpdatedBackoff = update_backoff(Backoff),
					enqueue({block, {H, H2, TXRoot}}, UpdatedBackoff, UpdatedQueue);
				{ok, B} ->
					gen_server:cast(self(), {add_block, B}),
					UpdatedQueue
			end
	end.

enqueue(Item, Backoff, Queue) ->
	queue:in({Item, Backoff}, Queue).

update_backoff({_Timestamp, Interval}) ->
	UpdatedInterval = min(?MAX_BACKOFF_INTERVAL_S, Interval * 2),
	{os:system_time(second) + UpdatedInterval, UpdatedInterval}.

read_block(H, H2, TXRoot) ->
	case ar_storage:read_block(H) of
		unavailable ->
			download_block(H, H2, TXRoot);
		B ->
			download_txs(B, TXRoot)
	end.

download_block(H, H2, TXRoot) ->
	Fork_2_0 = ar_fork:height_2_0(),
	case ar_network:get_block_shadow(H) of
		unavailable ->
			?LOG_WARNING([
				{event, ar_header_sync_failed_to_download_block_header},
				{block, ar_util:encode(H)}
			]),
			error;
		#block{ height = Height } = B ->
			BH =
				case Height >= Fork_2_0 of
					true ->
						ar_weave:indep_hash(B);
					false ->
						ar_weave:indep_hash(
							B#block{ tx_root = TXRoot, txs = lists:sort(B#block.txs) }
						)
				end,
			case BH of
				H when Height >= Fork_2_0 ->
					download_txs(B, TXRoot);
				H2 when Height < Fork_2_0 ->
					download_txs(B, TXRoot);
				_ ->
					?LOG_WARNING([
						{event, ar_header_sync_block_hash_mismatch},
						{block, ar_util:encode(H)}
					]),
					{error, block_hash_mismatch}
			end
	end.

download_txs(B, TXRoot) ->
	case ar_network:get_txs(B#block.txs) of
		unavailable ->
			?LOG_WARNING([
				{event, ar_header_sync_block_tx_not_found},
				{block, ar_util:encode(B#block.indep_hash)}
			]),
			error;
		{partial,_,_} ->
			?LOG_WARNING([
				{event, ar_header_sync_block_tx_not_found},
				{block, ar_util:encode(B#block.indep_hash)}
			]),
			error;
		TXs ->
			SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs),
			SizeTaggedDataRoots =
				[{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
			{Root, _Tree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
			case Root of
				TXRoot ->
					ar_data_sync:add_block(B, SizeTaggedTXs),
					case move_data_to_v2_index(TXs) of
						ok ->
							{ok, B#block{ txs = TXs }};
						{error, Reason} ->
							?LOG_WARNING([
								{event, ar_header_sync_failed_to_migrate_v1_txs},
								{block, ar_util:encode(B#block.indep_hash)},
								{reason, Reason}
							]),
							error
					end;
				_ ->
						?LOG_WARNING([
							{event, ar_header_sync_block_tx_root_mismatch},
							{block, ar_util:encode(B#block.indep_hash)}
						]),
						error
			end
	end.

move_data_to_v2_index(TXs) ->
	%% Migrate the transaction data to the new index for blocks
	%% written prior to this update.
	lists:foldl(
		fun (#tx{ format = 2, data_size = DataSize } = TX, ok) when DataSize > 0 ->
				case ar_storage:read_tx_data(TX) of
					{error, enoent} ->
						ok;
					{ok, Data} ->
						case ar_storage:write_tx_data(
									no_expected_data_root,
									Data,
									write_to_free_space_buffer
								) of
							ok ->
								file:delete(ar_storage:tx_data_filepath(TX));
							Error ->
								Error
						end;
					Error ->
						Error
				end;
			(#tx{ format = 1, id = ID, data_size = DataSize } = TX, ok) when DataSize > 0 ->
				case ar_storage:lookup_tx_filename(ID) of
					unavailable ->
						ok;
					{migrated_v1, _} ->
						ok;
					{ok, _} ->
						case ar_storage:write_tx(TX) of
							ok ->
								case file:delete(ar_storage:tx_filepath(TX)) of
									{error, enoent} ->
										ok;
									ok ->
										ok;
									Error ->
										Error
								end;
							Error ->
								Error
						end
				end;
			(_, Acc) ->
				Acc
		end,
		ok,
		TXs
	).
