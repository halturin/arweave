-ifndef(AR_CONFIG_HRL).
-define(AR_CONFIG_HRL, true).

-include_lib("ar.hrl").

-record(config_webhook, {
	events = [],
	url = undefined,
	headers = []
}).

%% @doc The polling frequency in seconds.
-ifdef(DEBUG).
-define(DEFAULT_POLLING_INTERVAL, 5).
-else.
-define(DEFAULT_POLLING_INTERVAL, 60).
-endif.

%% @doc The number of data sync jobs to run. Each job periodically picks a range
%% and downloads it from peers.
%% @end
-define(DEFAULT_SYNC_JOBS, 2).

%% @doc The default expiration time for a data root in the disk pool.
-define(DEFAULT_DISK_POOL_DATA_ROOT_EXPIRATION_TIME_S, 2 * 60 * 60).

%% @doc The default size limit for unconfirmed chunks, per data root.
-define(DEFAULT_MAX_DISK_POOL_DATA_ROOT_BUFFER_MB, 50).

%% @doc The default total size limit for unconfirmed chunks.
-ifdef(DEBUG).
-define(DEFAULT_MAX_DISK_POOL_BUFFER_MB, 100).
-else.
-define(DEFAULT_MAX_DISK_POOL_BUFFER_MB, 2000).
-endif.

%% Start options with default values.
-record(config, {
	init = false,
	port = ?DEFAULT_HTTP_IFACE_PORT,
	mine = false,
	peers = [],
	data_dir = ".",
	metrics_dir = ?METRICS_DIR,
	polling = ?DEFAULT_POLLING_INTERVAL, % Polling frequency in seconds.
	auto_join = true,
	clean = false,
	diff = ?DEFAULT_DIFF,
	mining_addr = false,
	max_miners = ?NUM_MINING_PROCESSES,
	max_emitters = ?NUM_EMITTER_PROCESSES,
	tx_propagation_parallelization = ?TX_PROPAGATION_PARALLELIZATION,
	sync_jobs = ?DEFAULT_SYNC_JOBS,
	new_key = false,
	load_key = false,
	disk_space,
	used_space = 0,
	start_from_block_index = false,
	internal_api_secret = not_set,
	enable = [],
	disable = [],
	transaction_blacklist_files = [],
	transaction_blacklist_urls = [],
	transaction_whitelist_files = [],
	transaction_whitelist_urls = [],
	gateway_domain = not_set,
	gateway_custom_domains = [],
	requests_per_minute_limit = ?DEFAULT_REQUESTS_PER_MINUTE_LIMIT,
	max_propagation_peers = ?DEFAULT_MAX_PROPAGATION_PEERS,
	ipfs_pin = false,
	webhooks = [],
	max_connections = 1024,
	max_gateway_connections = 128,
	max_poa_option_depth = 500,
	disk_pool_data_root_expiration_time = ?DEFAULT_DISK_POOL_DATA_ROOT_EXPIRATION_TIME_S,
	max_disk_pool_buffer_mb = ?DEFAULT_MAX_DISK_POOL_BUFFER_MB,
	max_disk_pool_data_root_buffer_mb = ?DEFAULT_MAX_DISK_POOL_DATA_ROOT_BUFFER_MB,
	randomx_bulk_hashing_iterations = 12,
	rates = #{},
	triggers = #{}
}).

-endif.
