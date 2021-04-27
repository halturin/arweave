%% @doc The size in bytes of a portion of the disk space reserved to account for the lag
%% between getting close to no available space and receiving the information about it.
%% The node would only sync data if it has at least so much of the available space.
-ifdef(DEBUG).
-define(DISK_DATA_BUFFER_SIZE, 30 * 1024 * 1024).
-else.
-define(DISK_DATA_BUFFER_SIZE, 20 * 1024 * 1024 * 1024). % >15 GiB ~5 mins of syncing at 60 MiB/s
-endif.

%% @doc The number of peer sync records to consult each time we look for an interval to sync.
-define(CONSULT_PEER_RECORDS_COUNT, 5).
%% @doc The number of best peers to pick ?CONSULT_PEER_RECORDS_COUNT from, to fetch the
%% corresponding number of sync records.
-define(PICK_PEERS_OUT_OF_RANDOM_N, 20).

%% @doc The size in bits of the offset key in kv databases.
-define(OFFSET_KEY_BITSIZE, 256).

%% @doc The size in bits of the key prefix used in prefix bloom filter
%% when looking up chunks by offsets from kv database.
%% 29 bytes of the prefix correspond to the 16777216 (16 Mib) max distance
%% between the keys with the same prefix. The prefix should be bigger than
%% max chunk size (256 KiB) so that the chunk in question is likely to be
%% found in the filter and smaller than an SST table (200 MiB) so that the
%% filter lookup can narrow the search down to a single table. @end
-define(OFFSET_KEY_PREFIX_BITSIZE, 232).

%% @doc The number of block confirmations to track. When the node
%% joins the network or a chain reorg occurs, it uses its record about
%% the last ?TRACK_CONFIRMATIONS blocks and the new block index to
%% determine the orphaned portion of the weave.
-define(TRACK_CONFIRMATIONS, ?STORE_BLOCKS_BEHIND_CURRENT * 2).

%% @doc The maximum number of synced intervals shared with peers.
-ifdef(DEBUG).
-define(MAX_SHARED_SYNCED_INTERVALS_COUNT, 20).
-else.
-define(MAX_SHARED_SYNCED_INTERVALS_COUNT, 10000).
-endif.

%% @doc The upper limit for the size of a sync record serialized using Erlang Term Format.
-define(MAX_ETF_SYNC_RECORD_SIZE, 80 * ?MAX_SHARED_SYNCED_INTERVALS_COUNT).

%% @doc The upper size limit for a serialized chunk with its proof
%% as it travels around the network.
%%
%% It is computed as ?MAX_PATH_SIZE (data_path) + DATA_CHUNK_SIZE (chunk) +
%% 32 * 1000 (tx_path, considering the 1000 txs per block limit),
%% multiplied by 1.34 (Base64), rounded to the nearest 50000 -
%% the difference is sufficient to fit an offset, a data_root,
%% and special JSON chars.
-define(MAX_SERIALIZED_CHUNK_PROOF_SIZE, 750000).

%% @doc Transaction data bigger than this limit is not served in
%% GET /tx/<id>/data endpoint. Clients interested in downloading
%% such data should fetch it chunk by chunk.
-define(MAX_SERVED_TX_DATA_SIZE, 12 * 1024 * 1024).

%% @doc The time to wait until the next full disk pool scan.
-ifdef(DEBUG).
-define(DISK_POOL_SCAN_FREQUENCY_MS, 2000).
-else.
-define(DISK_POOL_SCAN_FREQUENCY_MS, 120000).
-endif.

%% @doc The frequency of removing expired data roots from the disk pool.
-define(REMOVE_EXPIRED_DATA_ROOTS_FREQUENCY_MS, 60000).

%% @doc Time to wait before retrying a failed migration step.
-define(MIGRATION_RETRY_DELAY_MS, 10000).

%% @doc The frequency of storing the server state on disk.
-define(STORE_STATE_FREQUENCY_MS, 30000).

