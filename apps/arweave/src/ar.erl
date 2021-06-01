%%%
%%% @doc Arweave server entrypoint and basic utilities.
%%%
-module(ar).

-behaviour(application).

-export([
	main/0, main/1, start/0, start/1, start/2, stop/1, stop_dependencies/0,
	tests/0, tests/1, tests/2,
	test_ipfs/0,
	docs/0,
	start_for_tests/0,
	shutdown/1,
	console/1, console/2
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

%% A list of the modules to test.
%% At some point we might want to make this just test all mods starting with
%% ar_.
-define(
	CORE_TEST_MODS,
	[
		ar, % ok
		ar_meta_db, % ok
		ar_webhook_tests, % ok
		ar_poller_tests, % almost ok with timeout on the last step
		ar_kv, % ok
		ar_block_cache, % ok
		ar_unbalanced_merkle, % ok
		ar_intervals, % ok
		ar_ets_intervals, % ok
		ar_patricia_tree, % ok
		ar_diff_dag, % ok
		ar_config_tests, % ok
		ar_deep_hash, % ok
		ar_inflation, % ok
		ar_util, % ok
		ar_base64_compatibility_tests, % ok
		ar_storage, % ok
		ar_merkle, % ok
		ar_semaphore_tests, % ok
		ar_tx_db, % ok
		ar_tx, % ok
		ar_wallet, % ok
		ar_serialize, % ok
		ar_block, % ok
		ar_difficulty_tests, % ok
		ar_retarget, % fails
		ar_weave, % no tests to run
		ar_tx_blacklist_tests, % fails
		ar_data_sync_tests, % ok
		ar_header_sync_tests, % fails with the same issue as on ar_retarget
		ar_poa_tests, % ok
		ar_node_tests, % ok
		ar_fork_recovery_tests, % ok
		ar_mine, % ok
		ar_tx_replay_pool_tests, %ok
		ar_tx_queue, % almost ok, 2 tests fail
		ar_http_iface_tests,
		ar_multiple_txs_per_wallet_tests, % ok, but it has race conditional issue
		ar_pricing, % ok
		ar_gateway_middleware_tests, % ok
		ar_http_util_tests, % ok
		ar_mine_randomx_tests % ok
	]
).

%% Supported feature flags (default behaviour)
% http_logging (false)
% disk_logging (false)
% miner_logging (true)
% subfield_queries (false)
% blacklist (true)
% time_syncing (true)

%% @doc Command line program entrypoint. Takes a list of arguments.
main() ->
	show_help().

main("") ->
	show_help();
main(Args) ->
	start(parse_config_file(Args, [], #config{})).

show_help() ->
	io:format("Usage: arweave-server [options]~n"),
	io:format("Compatible with network: ~s~n", [?NETWORK_NAME]),
	io:format("Options:~n"),
	lists:foreach(
		fun({Opt, Desc}) ->
			io:format("\t~s~s~n",
				[
					string:pad(Opt, 40, trailing, $ ),
					Desc
				]
			)
		end,
		[
			{"config_file (path)", "Load configuration from specified file."},
			{"peer (ip:port)", "Join a network on a peer (or set of peers)."},
			{"start_from_block_index", "Start the node from the latest stored block index."},
			{"mine", "Automatically start mining once the netwok has been joined."},
			{"port", "The local port to use for mining. "
						"This port must be accessible by remote peers."},
			{"data_dir", "The directory for storing the weave and the wallets (when generated)."},
			{"metrics_dir", "The directory for persisted metrics."},
			{"polling (num)", lists:flatten(
					io_lib:format(
						"Poll peers for new blocks every N seconds. Default is ~p. "
						"Useful in environments where port forwarding is not possible.",
						[?DEFAULT_POLLING_INTERVAL]
					)
			)},
			{"clean", "Clear the block cache before starting."},
			{"no_auto_join", "Do not automatically join the network of your peers."},
			{"mining_addr (addr)", "The address that mining rewards should be credited to."},
			{"max_miners (num)",
				io_lib:format(
					"The maximum number of mining processes. Default: ~B.",
					[?NUM_MINING_PROCESSES]
				)},
			{"tx_propagation_parallelization (num)", "The maximum number of best peers to propagate transactions to at a time (default 4)."},
			{"max_propagation_peers (num)", "The maximum number of best peers to propagate blocks and transactions to. Default is 50."},
			{"sync_jobs (num)",
				io_lib:format(
					"The number of data syncing jobs to run. Default: ~B."
					" Each job periodically picks a range and downloads it from peers.",
					[?DEFAULT_SYNC_JOBS]
				)},
			{"new_mining_key", "Generate a new keyfile, apply it as the reward address"},
			{"load_mining_key (file)", "Load the address that mining rewards should be credited to from file."},
			{"ipfs_pin", "Pin incoming IPFS tagged transactions on your local IPFS node."},
			{"transaction_blacklist (file)", "A file containing blacklisted transactions. "
											 "One Base64 encoded transaction ID per line."},
			{"transaction_blacklist_url", "An HTTP endpoint serving a transaction blacklist."},
			{"transaction_whitelist (file)", "A file containing whitelisted transactions. "
											 "One Base64 encoded transaction ID per line. "
											 "If a transaction is in both lists, it is "
											 "considered whitelisted."},
			{"transaction_whitelist_url", "An HTTP endpoint serving a transaction whitelist."},
			{"disk_space (num)",
				"Max size (in GB) for the disk partition containing "
				"the Arweave data directory (blocks, txs, etc) when "
				"the miner stops writing files to disk."},
			{"disk_space_check_frequency (num)",
				io_lib:format(
					"The frequency in seconds of requesting the information "
					"about the available disk space from the operating system, "
					"used to decide on whether to continue syncing the historical "
					"data or clean up some space. Default is ~B.",
					[?DISK_SPACE_CHECK_FREQUENCY_MS div 1000]
				)},
			{"init", "Start a new weave."},
			{"internal_api_secret (secret)",
				lists:flatten(
					io_lib:format(
						"Enables the internal API endpoints, only accessible with this secret. Min. ~B chars.",
						[?INTERNAL_API_SECRET_MIN_LEN]
					)
				)
			},
			{"enable (feature)", "Enable a specific (normally disabled) feature. For example, subfield_queries."},
			{"disable (feature)", "Disable a specific (normally enabled) feature."},
			{"gateway (domain)", "Run a gateway on the specified domain"},
			{"custom_domain (domain)", "Add a domain to the list of supported custom domains."},
			{"requests_per_minute_limit (number)", "Limit the maximum allowed number of HTTP requests per IP address per minute. Default is 900."},
			{"max_connections", "The number of connections to be handled concurrently. Its purpose is to prevent your system from being overloaded and ensuring all the connections are handled optimally. Default is 1024."},
			{"max_gateway_connections", "The number of gateway connections to be handled concurrently. Default is 128."},
			{"max_poa_option_depth",
				"The number of PoA alternatives to try until the recall data is "
				"found. Has to be an integer > 1. The mining difficulty grows linearly "
				"as a function of the alternative as (0.75 + 0.25 * number) * diff, "
				"up to (0.75 + 0.25 * max_poa_option_depth) * diff. Default is 500."},
			{"disk_pool_data_root_expiration_time",
				"The time in seconds of how long a pending or orphaned data root is kept in the disk pool. The
				default is 2 * 60 * 60 (2 hours)."},
			{"max_disk_pool_buffer_mb",
				"The max total size in mebibytes of the pending chunks in the disk pool."
				"The default is 2000 (2 GiB)."},
			{"max_disk_pool_data_root_buffer_mb",
				"The max size in mebibytes per data root of the pending chunks in the disk"
				" pool. The default is 50."},
			{"randomx_bulk_hashing_iterations",
				"The number of hashes RandomX generates before reporting the result back"
				" to the Arweave miner. The faster CPU hashes, the higher this value should be."
			},
			{"debug",
				"Enable extended logging."
			}
		]
	),
	erlang:halt().

parse_config_file(["config_file", Path | Rest], Skipped, _) ->
	case read_config_from_file(Path) of
		{ok, Config} ->
			parse_config_file(Rest, Skipped, Config);
		{error, Reason, Item} ->
			io:format("Failed to parse config: ~p: ~p.~n", [Reason, Item]),
			show_help();
		{error, Reason} ->
			io:format("Failed to parse config: ~p.~n", [Reason]),
			show_help()
	end;
parse_config_file([Arg | Rest], Skipped, Config) ->
	parse_config_file(Rest, [Arg | Skipped], Config);
parse_config_file([], Skipped, Config) ->
	Args = lists:reverse(Skipped),
	parse_cli_args(Args, Config).

read_config_from_file(Path) ->
	case file:read_file(Path) of
		{ok, FileData} -> ar_config:parse(FileData);
		{error, _} -> {error, file_unreadable, Path}
	end.

parse_cli_args([], C) -> C;
parse_cli_args(["mine"|Rest], C) ->
	parse_cli_args(Rest, C#config { mine = true });
parse_cli_args(["peer", Peer|Rest], C = #config { peers = Ps }) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ValidPeer} ->
			parse_cli_args(Rest, C#config { peers = [ValidPeer|Ps] });
		{error, _} ->
			io:format("Peer ~p invalid ~n", [Peer]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["transaction_blacklist", File|Rest], C = #config { transaction_blacklist_files = Files } ) ->
	parse_cli_args(Rest, C#config { transaction_blacklist_files = [File|Files] });
parse_cli_args(["transaction_blacklist_url", URL | Rest], C = #config { transaction_blacklist_urls = URLs} ) ->
	parse_cli_args(Rest, C#config{ transaction_blacklist_urls = [URL | URLs] });
parse_cli_args(["transaction_whitelist", File|Rest], C = #config { transaction_whitelist_files = Files } ) ->
	parse_cli_args(Rest, C#config { transaction_whitelist_files = [File|Files] });
parse_cli_args(["transaction_whitelist_url", URL | Rest], C = #config { transaction_whitelist_urls = URLs} ) ->
	parse_cli_args(Rest, C#config { transaction_whitelist_urls = [URL | URLs] });
parse_cli_args(["port", Port|Rest], C) ->
	parse_cli_args(Rest, C#config { port = list_to_integer(Port) });
parse_cli_args(["data_dir", DataDir|Rest], C) ->
	parse_cli_args(Rest, C#config { data_dir = DataDir });
parse_cli_args(["metrics_dir", MetricsDir|Rest], C) ->
	parse_cli_args(Rest, C#config { metrics_dir = MetricsDir });
parse_cli_args(["polling", Frequency|Rest], C) ->
	parse_cli_args(Rest, C#config { polling = list_to_integer(Frequency) });
parse_cli_args(["clean"|Rest], C) ->
	parse_cli_args(Rest, C#config { clean = true });
parse_cli_args(["no_auto_join"|Rest], C) ->
	parse_cli_args(Rest, C#config { auto_join = false });
parse_cli_args(["mining_addr", Addr|Rest], C) ->
	parse_cli_args(Rest, C#config { mining_addr = ar_util:decode(Addr) });
parse_cli_args(["max_miners", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { max_miners = list_to_integer(Num) });
parse_cli_args(["new_mining_key"|Rest], C)->
	parse_cli_args(Rest, C#config { new_key = true });
parse_cli_args(["disk_space", Size|Rest], C) ->
	parse_cli_args(Rest, C#config { disk_space = (list_to_integer(Size) * 1024 * 1024 * 1024) });
parse_cli_args(["disk_space_check_frequency", Frequency|Rest], C) ->
	parse_cli_args(Rest, C#config{
		disk_space_check_frequency = list_to_integer(Frequency) * 1000
	});
parse_cli_args(["load_mining_key", File|Rest], C) ->
	parse_cli_args(Rest, C#config { load_key = File });
parse_cli_args(["ipfs_pin" | Rest], C) ->
	parse_cli_args(Rest, C#config { ipfs_pin = true });
parse_cli_args(["start_from_block_index"|Rest], C) ->
	parse_cli_args(Rest, C#config { start_from_block_index = true });
parse_cli_args(["init"|Rest], C)->
	parse_cli_args(Rest, C#config { init = true });
parse_cli_args(["internal_api_secret", Secret | Rest], C) when length(Secret) >= ?INTERNAL_API_SECRET_MIN_LEN ->
	parse_cli_args(Rest, C#config { internal_api_secret = list_to_binary(Secret)});
parse_cli_args(["internal_api_secret", _ | _], _) ->
	io:format(
		"~nThe internal_api_secret must be at least ~B characters long.~n~n",
		[?INTERNAL_API_SECRET_MIN_LEN]
	),
	erlang:halt();
parse_cli_args(["enable", Feature | Rest ], C = #config { enable = Enabled }) ->
	parse_cli_args(Rest, C#config { enable = [ list_to_atom(Feature) | Enabled ] });
parse_cli_args(["disable", Feature | Rest ], C = #config { disable = Disabled }) ->
	parse_cli_args(Rest, C#config { disable = [ list_to_atom(Feature) | Disabled ] });
parse_cli_args(["gateway", Domain | Rest ], C) ->
	parse_cli_args(Rest, C#config { gateway_domain = list_to_binary(Domain) });
parse_cli_args(["custom_domain", Domain|Rest], C = #config { gateway_custom_domains = Ds }) ->
	parse_cli_args(Rest, C#config { gateway_custom_domains = [ list_to_binary(Domain) | Ds ] });
parse_cli_args(["requests_per_minute_limit", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { requests_per_minute_limit = list_to_integer(Num) });
parse_cli_args(["max_propagation_peers", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { max_propagation_peers = list_to_integer(Num) });
parse_cli_args(["sync_jobs", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { sync_jobs = list_to_integer(Num) });
parse_cli_args(["tx_propagation_parallelization", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { tx_propagation_parallelization = list_to_integer(Num) });
parse_cli_args(["max_connections", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { max_connections = list_to_integer(Num) });
parse_cli_args(["max_gateway_connections", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { max_gateway_connections = list_to_integer(Num) });
parse_cli_args(["max_poa_option_depth", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { max_poa_option_depth = list_to_integer(Num) });
parse_cli_args(["disk_pool_data_root_expiration_time", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { disk_pool_data_root_expiration_time = list_to_integer(Num) });
parse_cli_args(["max_disk_pool_buffer_mb", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { max_disk_pool_buffer_mb = list_to_integer(Num) });
parse_cli_args(["max_disk_pool_data_root_buffer_mb", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { max_disk_pool_data_root_buffer_mb = list_to_integer(Num) });
parse_cli_args(["randomx_bulk_hashing_iterations", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { randomx_bulk_hashing_iterations = list_to_integer(Num) });
parse_cli_args(["debug" | Rest], C) ->
	parse_cli_args(Rest, C#config { debug = true });
parse_cli_args([Arg|_Rest], _O) ->
	io:format("~nUnknown argument: ~s.~n", [Arg]),
	show_help().

%% @doc Start an Arweave node on this BEAM.
start() ->
	start(?DEFAULT_HTTP_IFACE_PORT).
start(Port) when is_integer(Port) ->
	start(#config { port = Port });
start(Config) ->
	%% Start the logging system.
	filelib:ensure_dir(?LOG_DIR ++ "/"),
	warn_if_single_scheduler(),
	ok = application:set_env(arweave, config, Config),
	{ok, _} = application:ensure_all_started(arweave, permanent).

start(normal, _Args) ->
	{ok, Config} = application:get_env(arweave, config),
	%% Configure logging for console output.
	LoggerFormatterConsole = #{
		legacy_header => false,
		single_line => true,
		chars_limit => 512,
		max_size => 512,
		depth => 16,
		template => [time," [",level,"] ",file,":",line," ",msg,"\n"]
	},
	logger:set_handler_config(default, formatter, {logger_formatter, LoggerFormatterConsole}),
	logger:set_handler_config(default, level, error),
	%% Configure logging to the logfile.
	LoggerConfigDisk = #{
		file => lists:flatten("logs/" ++ atom_to_list(node())),
		type => wrap,
		max_no_files => 10,
		max_no_bytes => 51418800 % 10 x 5MB
	},
	logger:add_handler(
		disk_log,
		logger_disk_log_h,
		#{ config => LoggerConfigDisk, level => debug}
	),
	LoggerFormatterDisk = #{
		%chars_limit => 512,
		%max_size => 512,
		%depth => 32,
		legacy_header => false,
		single_line => true,
		template => [time," [",level,"] ",file,":",line," ",msg,"\n"]
	},
	logger:set_handler_config(disk_log, formatter, {logger_formatter, LoggerFormatterDisk}),
	case Config#config.debug of
		true ->
			logger:set_application_level(arweave, debug);
		_ ->
			logger:set_application_level(arweave, info)
	end,
	%% Start the Prometheus metrics subsystem.
	prometheus_registry:register_collector(prometheus_process_collector),
	prometheus_registry:register_collector(ar_metrics_collector),
	%% Register custom metrics.
	ar_metrics:register(Config#config.metrics_dir),
	%% Verify port collisions when gateway is enabled.
	case {Config#config.port, Config#config.gateway_domain} of
		{P, D} when is_binary(D) andalso (P == 80 orelse P == 443) ->
			io:format(
				"~nThe port must be different than 80 or 443 when the gateway is enabled.~n~n"),
			erlang:halt();
		_ ->
			do_nothing
	end,
	%% Start other apps which we depend on.
	ok = prepare_graphql(),
	case Config#config.ipfs_pin of
		false -> ok;
		true  -> app_ipfs:start_pinning()
	end,
	%% Start Arweave.
	ar_sup:start_link().

shutdown([NodeName]) ->
	rpc:cast(NodeName, init, stop, []).

stop(_State) ->
	ok.

stop_dependencies() ->
	{ok, [_Kernel, _Stdlib, _SASL, _OSMon | Deps]} = application:get_key(arweave, applications),
	lists:foreach(fun(Dep) -> ok = application:stop(Dep) end, Deps).

prepare_graphql() ->
	ok = ar_graphql:load_schema(),
	ok.

%% One scheduler => one dirty scheduler => Calculating a RandomX hash, e.g.
%% for validating a block, will be blocked on initializing a RandomX dataset,
%% which takes minutes.
warn_if_single_scheduler() ->
	case erlang:system_info(schedulers_online) of
		1 ->
			?LOG_WARNING(
				"WARNING: Running only one CPU core / Erlang scheduler may cause issues.");
		_ ->
			ok
	end.

%% @doc Run all of the tests associated with the core project.
tests() ->
	tests(?CORE_TEST_MODS, #config {init = true, debug = true}).

tests(Mods, Config) when is_list(Mods) ->
	start_for_tests(Config),
	case eunit:test({timeout, ?TEST_TIMEOUT, [Mods]}, [verbose]) of
		ok ->
			ok;
		_ ->
			exit(tests_failed)
	end.

start_for_tests() ->
	start_for_tests(#config { }).

start_for_tests(Config) ->
	start(Config#config {
		peers = [],
		data_dir = "data_test_master",
		metrics_dir = "metrics_master",
		disable = [randomx_jit]
	}).

%% @doc Run the tests for a set of module(s).
%% Supports strings so that it can be trivially induced from a unix shell call.
tests(Mod) when not is_list(Mod) -> tests([Mod]);
tests(Args) ->
	Mods =
		lists:map(
			fun(Mod) when is_atom(Mod) -> Mod;
			   (Str) -> list_to_atom(Str)
			end,
			Args
		),
	tests(Mods, #config {}).

%% @doc Run the tests for the IPFS integration. Requires a running local IPFS node.
test_ipfs() ->
	Mods = [app_ipfs_tests],
	tests(Mods, #config{}).

%% @doc Generate the project documentation.
docs() ->
	Mods =
		lists:filter(
			fun(File) -> filename:extension(File) == ".erl" end,
			element(2, file:list_dir("apps/arweave/src"))
		),
	edoc:files(["apps/arweave/src/" ++ Mod || Mod <- Mods], [{dir, "source_code_docs"}]).

%% @doc Ensure that parsing of core command line options functions correctly.
commandline_parser_test_() ->
	{timeout, 20, fun() ->
		Addr = crypto:strong_rand_bytes(32),
		Tests =
			[
				{"peer 1.2.3.4 peer 5.6.7.8:9", #config.peers, [{5,6,7,8,9},{1,2,3,4,1984}]},
				{"mine", #config.mine, true},
				{"port 22", #config.port, 22},
				{"mining_addr "
					++ binary_to_list(ar_util:encode(Addr)), #config.mining_addr, Addr}
			],
		X = string:split(string:join([ L || {L, _, _} <- Tests ], " "), " ", all),
		C = parse_cli_args(X, #config{}),
		lists:foreach(
			fun({_, Index, Value}) ->
				?assertEqual(element(Index, C), Value)
			end,
			Tests
		)
	end}.

-ifdef(DEBUG).
console(_) ->
	ok.

console(_, _) ->
	ok.
-else.
console(Format) ->
	io:format(Format).

console(Format, Params) ->
	io:format(Format, Params).
-endif.
