-module(ar_config).

-export([parse/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

parse(Config) when is_binary(Config) ->
	case ar_serialize:json_decode(Config) of
		{ok, JsonValue} -> parse_options(JsonValue);
		{error, _} -> {error, bad_json, Config}
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================


parse_options({KVPairs}) when is_list(KVPairs) ->
	parse_options(KVPairs, #config{});
parse_options(JsonValue) ->
	{error, root_not_object, JsonValue}.

parse_options([{_, null} | Rest], Config) ->
	parse_options(Rest, Config);

parse_options([{<<"config_file">>, _} | _], _) ->
	{error, config_file_set};

parse_options([{<<"peers">>, Peers} | Rest], Config) when is_list(Peers) ->
	case parse_peers(Peers, []) of
		{ok, ParsedPeers} ->
			parse_options(Rest, Config#config{ peers = ParsedPeers });
		error ->
			{error, bad_peers, Peers}
	end;
parse_options([{<<"peers">>, Peers} | _], _) ->
	{error, {bad_type, peers, array}, Peers};

parse_options([{<<"start_from_block_index">>, true} | Rest], Config) ->
	parse_options(Rest, Config#config{ start_from_block_index = true });
parse_options([{<<"start_from_block_index">>, false} | Rest], Config) ->
	parse_options(Rest, Config#config{ start_from_block_index = false });
parse_options([{<<"start_from_block_index">>, Opt} | _], _) ->
	{error, {bad_type, start_from_block_index, boolean}, Opt};

parse_options([{<<"mine">>, true} | Rest], Config) ->
	parse_options(Rest, Config#config{ mine = true });
parse_options([{<<"mine">>, false} | Rest], Config) ->
	parse_options(Rest, Config);
parse_options([{<<"mine">>, Opt} | _], _) ->
	{error, {bad_type, mine, boolean}, Opt};

parse_options([{<<"port">>, Port} | Rest], Config) when is_integer(Port) ->
	parse_options(Rest, Config#config{ port = Port });
parse_options([{<<"port">>, Port} | _], _) ->
	{error, {bad_type, port, number}, Port};

parse_options([{<<"data_dir">>, DataDir} | Rest], Config) when is_binary(DataDir) ->
	parse_options(Rest, Config#config{ data_dir = binary_to_list(DataDir) });
parse_options([{<<"data_dir">>, DataDir} | _], _) ->
	{error, {bad_type, data_dir, string}, DataDir};

parse_options([{<<"metrics_dir">>, MetricsDir} | Rest], Config) when is_binary(MetricsDir) ->
	parse_options(Rest, Config#config { metrics_dir = binary_to_list(MetricsDir) });
parse_options([{<<"metrics_dir">>, MetricsDir} | _], _) ->
	{error, {bad_type, metrics_dir, string}, MetricsDir};

parse_options([{<<"polling">>, Frequency} | Rest], Config) when is_integer(Frequency) ->
	parse_options(Rest, Config#config{ polling = Frequency });
parse_options([{<<"polling">>, Opt} | _], _) ->
	{error, {bad_type, polling, number}, Opt};

parse_options([{<<"clean">>, true} | Rest], Config) ->
	parse_options(Rest, Config#config{ clean = true });
parse_options([{<<"clean">>, false} | Rest], Config) ->
	parse_options(Rest, Config);
parse_options([{<<"clean">>, Opt} | _], _) ->
	{error, {bad_type, clean, boolean}, Opt};

parse_options([{<<"no_auto_join">>, true} | Rest], Config) ->
	parse_options(Rest, Config#config{ auto_join = false });
parse_options([{<<"no_auto_join">>, false} | Rest], Config) ->
	parse_options(Rest, Config);
parse_options([{<<"no_auto_join">>, Opt} | _], _) ->
	{error, {bad_type, no_auto_join, boolean}, Opt};

parse_options([{<<"diff">>, Diff} | Rest], Config) when is_integer(Diff) ->
	parse_options(Rest, Config#config{ diff = Diff });
parse_options([{<<"diff">>, Diff} | _], _) ->
	{error, {bad_type, diff, number}, Diff};

parse_options([{<<"mining_addr">>, <<"unclaimed">>} | Rest], Config) ->
	parse_options(Rest, Config#config{ mining_addr = unclaimed });
parse_options([{<<"mining_addr">>, Addr} | Rest], Config) when is_binary(Addr) ->
	case ar_util:safe_decode(Addr) of
		{ok, D} -> parse_options(Rest, Config#config{ mining_addr = D });
		{error, _} -> {error, bad_mining_addr, Addr}
	end;
parse_options([{<<"mining_addr">>, Addr} | _], _) ->
	{error, {bad_type, mining_addr, string}, Addr};

parse_options([{<<"max_miners">>, MaxMiners} | Rest], Config) when is_integer(MaxMiners) ->
	parse_options(Rest, Config#config{ max_miners = MaxMiners });
parse_options([{<<"max_miners">>, MaxMiners} | _], _) ->
	{error, {bad_type, max_miners, number}, MaxMiners};

parse_options([{<<"io_threads">>, IOThreads} | Rest], Config) when is_integer(IOThreads) ->
	parse_options(Rest, Config#config{ io_threads = IOThreads });
parse_options([{<<"io_threads">>, IOThreads} | _], _) ->
	{error, {bad_type, io_threads, number}, IOThreads};

parse_options([{<<"stage_one_hashing_threads">>, HashingThreads} | Rest], Config)
		when is_integer(HashingThreads) ->
	parse_options(Rest, Config#config{ stage_one_hashing_threads = HashingThreads });
parse_options([{<<"stage_one_hashing_threads">>, HashingThreads} | _], _) ->
	{error, {bad_type, stage_one_hashing_threads, number}, HashingThreads};

parse_options([{<<"stage_two_hashing_threads">>, HashingThreads} | Rest], Config)
		when is_integer(HashingThreads) ->
	parse_options(Rest, Config#config{ stage_two_hashing_threads = HashingThreads });
parse_options([{<<"stage_two_hashing_threads">>, HashingThreads} | _], _) ->
	{error, {bad_type, stage_two_hashing_threads, number}, HashingThreads};

parse_options([{<<"max_emitters">>, Value} | Rest], Config) when is_integer(Value) ->
	parse_options(Rest, Config#config{ max_emitters = Value });
parse_options([{<<"max_emitters">>, Value} | _], _) ->
	{error, {bad_type, max_emitters, number}, Value};

parse_options([{<<"tx_propagation_parallelization">>, Value} | Rest], Config)
		when is_integer(Value) ->
	parse_options(Rest, Config#config{ tx_propagation_parallelization = Value });
parse_options([{<<"tx_propagation_parallelization">>, Value} | _], _) ->
	{error, {bad_type, tx_propagation_parallelization, number}, Value};

parse_options([{<<"max_propagation_peers">>, Value} | Rest], Config)
		when is_integer(Value) ->
	parse_options(Rest, Config#config{ max_propagation_peers = Value });
parse_options([{<<"max_propagation_peers">>, Value} | _], _) ->
	{error, {bad_type, max_propagation_peers, number}, Value};

parse_options([{<<"sync_jobs">>, Value} | Rest], Config)
		when is_integer(Value) ->
	parse_options(Rest, Config#config{ sync_jobs = Value });
parse_options([{<<"sync_jobs">>, Value} | _], _) ->
	{error, {bad_type, sync_jobs, number}, Value};

parse_options([{<<"header_sync_jobs">>, Value} | Rest], Config)
		when is_integer(Value) ->
	parse_options(Rest, Config#config{ header_sync_jobs = Value });
parse_options([{<<"header_sync_jobs">>, Value} | _], _) ->
	{error, {bad_type, header_sync_jobs, number}, Value};

parse_options([{<<"load_mining_key">>, DataDir} | Rest], Config) when is_binary(DataDir) ->
	parse_options(Rest, Config#config{ load_key = binary_to_list(DataDir) });
parse_options([{<<"load_mining_key">>, DataDir} | _], _) ->
	{error, {bad_type, load_mining_key, string}, DataDir};

parse_options([{<<"transaction_blacklists">>, TransactionBlacklists} | Rest], Config)
		when is_list(TransactionBlacklists) ->
	case safe_map(fun binary_to_list/1, TransactionBlacklists) of
		{ok, TransactionBlacklistStrings} ->
			parse_options(Rest, Config#config{
				transaction_blacklist_files = TransactionBlacklistStrings
			});
		error ->
			{error, bad_transaction_blacklists}
	end;
parse_options([{<<"transaction_blacklists">>, TransactionBlacklists} | _], _) ->
	{error, {bad_type, transaction_blacklists, array}, TransactionBlacklists};

parse_options([{<<"transaction_blacklist_urls">>, TransactionBlacklistURLs} | Rest], Config)
		when is_list(TransactionBlacklistURLs) ->
	case safe_map(fun binary_to_list/1, TransactionBlacklistURLs) of
		{ok, TransactionBlacklistURLStrings} ->
			parse_options(Rest, Config#config{
				transaction_blacklist_urls = TransactionBlacklistURLStrings
			});
		error ->
			{error, bad_transaction_blacklist_urls}
	end;
parse_options([{<<"transaction_blacklist_urls">>, TransactionBlacklistURLs} | _], _) ->
	{error, {bad_type, transaction_blacklist_urls, array}, TransactionBlacklistURLs};

parse_options([{<<"transaction_whitelists">>, TransactionWhitelists} | Rest], Config)
		when is_list(TransactionWhitelists) ->
	case safe_map(fun binary_to_list/1, TransactionWhitelists) of
		{ok, TransactionWhitelistStrings} ->
			parse_options(Rest, Config#config{
				transaction_whitelist_files = TransactionWhitelistStrings
			});
		error ->
			{error, bad_transaction_whitelists}
	end;
parse_options([{<<"transaction_whitelists">>, TransactionWhitelists} | _], _) ->
	{error, {bad_type, transaction_whitelists, array}, TransactionWhitelists};

parse_options([{<<"transaction_whitelist_urls">>, TransactionWhitelistURLs} | Rest], Config)
		when is_list(TransactionWhitelistURLs) ->
	case safe_map(fun binary_to_list/1, TransactionWhitelistURLs) of
		{ok, TransactionWhitelistURLStrings} ->
			parse_options(Rest, Config#config{
				transaction_whitelist_urls = TransactionWhitelistURLStrings
			});
		error ->
			{error, bad_transaction_whitelist_urls}
	end;
parse_options([{<<"transaction_whitelist_urls">>, TransactionWhitelistURLs} | _], _) ->
	{error, {bad_type, transaction_whitelist_urls, array}, TransactionWhitelistURLs};

parse_options([{<<"disk_space">>, DiskSpace} | Rest], Config) when is_integer(DiskSpace) ->
	parse_options(Rest, Config#config{ disk_space = DiskSpace * 1024 * 1024 * 1024 });
parse_options([{<<"disk_space">>, DiskSpace} | _], _) ->
	{error, {bad_type, disk_space, number}, DiskSpace};

parse_options([{<<"disk_space_check_frequency">>, Frequency} | Rest], Config)
		when is_integer(Frequency) ->
	parse_options(Rest, Config#config{ disk_space_check_frequency = Frequency * 1000 });
parse_options([{<<"disk_space_check_frequency">>, Frequency} | _], _) ->
	{error, {bad_type, disk_space_check_frequency, number}, Frequency};

parse_options([{<<"ipfs_pin">>, false} | Rest], Config) ->
	parse_options(Rest, Config);
parse_options([{<<"ipfs_pin">>, true} | Rest], Config) ->
	parse_options(Rest, Config#config{ ipfs_pin = true });

parse_options([{<<"init">>, true} | Rest], Config) ->
	parse_options(Rest, Config#config{ init = true });
parse_options([{<<"init">>, false} | Rest], Config) ->
	parse_options(Rest, Config#config{ init = false });
parse_options([{<<"init">>, Opt} | _], _) ->
	{error, {bad_type, init, boolean}, Opt};

parse_options([{<<"internal_api_secret">>, Secret} | Rest], Config)
		when is_binary(Secret), byte_size(Secret) >= ?INTERNAL_API_SECRET_MIN_LEN ->
	parse_options(Rest, Config#config{ internal_api_secret = Secret });
parse_options([{<<"internal_api_secret">>, Secret} | _], _) ->
	{error, bad_secret, Secret};

parse_options([{<<"enable">>, Features} | Rest], Config) when is_list(Features) ->
	case safe_map(fun(Feature) -> binary_to_atom(Feature, latin1) end, Features) of
		{ok, FeatureAtoms} ->
			parse_options(Rest, Config#config{ enable = FeatureAtoms });
		error ->
			{error, bad_enable}
	end;
parse_options([{<<"enable">>, Features} | _], _) ->
	{error, {bad_type, enable, array}, Features};

parse_options([{<<"disable">>, Features} | Rest], Config) when is_list(Features) ->
	case safe_map(fun(Feature) -> binary_to_atom(Feature, latin1) end, Features) of
		{ok, FeatureAtoms} ->
			parse_options(Rest, Config#config{ disable = FeatureAtoms });
		error ->
			{error, bad_disable}
	end;
parse_options([{<<"disable">>, Features} | _], _) ->
	{error, {bad_type, disable, array}, Features};

parse_options([{<<"gateway">>, Domain} | Rest], Config) when is_binary(Domain) ->
	parse_options(Rest, Config#config{ gateway_domain = Domain });
parse_options([{<<"gateway">>, false} | Rest], Config) ->
	parse_options(Rest, Config);
parse_options([{<<"gateway">>, Gateway} | _], _) ->
	{error, {bad_type, gateway, string}, Gateway};

parse_options([{<<"custom_domains">>, CustomDomains} | Rest], Config)
		when is_list(CustomDomains) ->
	case lists:all(fun is_binary/1, CustomDomains) of
		true ->
			parse_options(Rest, Config#config{ gateway_custom_domains = CustomDomains });
		false ->
			{error, bad_custom_domains}
	end;
parse_options([{<<"custom_domains">>, CustomDomains} | _], _) ->
	{error, {bad_type, custom_domains, array}, CustomDomains};

parse_options([{<<"webhooks">>, WebhookConfigs} | Rest], Config) when is_list(WebhookConfigs) ->
	case parse_webhooks(WebhookConfigs, []) of
		{ok, ParsedWebhooks} ->
			parse_options(Rest, Config#config{ webhooks = ParsedWebhooks });
		error ->
			{error, bad_webhooks, WebhookConfigs}
	end;
parse_options([{<<"webhooks">>, Webhooks} | _], _) ->
	{error, {bad_type, webhooks, array}, Webhooks};

parse_options([{<<"semaphores">>, Semaphores} | Rest], Config) when is_tuple(Semaphores) ->
	case parse_semaphores(Semaphores, Config#config.semaphores) of
		{ok, ParsedSemaphores} ->
			parse_options(Rest, Config#config{ semaphores = ParsedSemaphores });
		error ->
			{error, bad_semaphores, Semaphores}
	end;
parse_options([{<<"semaphores">>, Semaphores} | _], _) ->
	{error, {bad_type, semaphores, object}, Semaphores};

parse_options([{<<"max_connections">>, MaxConnections} | Rest], Config)
		when is_integer(MaxConnections) ->
	parse_options(Rest, Config#config{ max_connections = MaxConnections });

parse_options([{<<"max_gateway_connections">>, MaxGatewayConnections} | Rest], Config)
		when is_integer(MaxGatewayConnections) ->
	parse_options(Rest, Config#config{ max_gateway_connections = MaxGatewayConnections });

parse_options([{<<"max_poa_option_depth">>, MaxPOAOptionDepth} | Rest], Config)
		when is_integer(MaxPOAOptionDepth) ->
	parse_options(Rest, Config#config{ max_poa_option_depth = MaxPOAOptionDepth });

parse_options([{<<"disk_pool_data_root_expiration_time">>, D} | Rest], Config)
		when is_integer(D) ->
	parse_options(Rest, Config#config{ disk_pool_data_root_expiration_time = D });

parse_options([{<<"max_disk_pool_buffer_mb">>, D} | Rest], Config) when is_integer(D) ->
	parse_options(Rest, Config#config{ max_disk_pool_buffer_mb= D });

parse_options([{<<"max_disk_pool_data_root_buffer_mb">>, D} | Rest], Config) when is_integer(D) ->
	parse_options(Rest, Config#config{ max_disk_pool_data_root_buffer_mb = D });

parse_options([{<<"randomx_bulk_hashing_iterations">>, D} | Rest], Config) when is_integer(D) ->
	parse_options(Rest, Config#config{ randomx_bulk_hashing_iterations = D });

parse_options([{<<"debug">>, B} | Rest], Config) when is_boolean(B) ->
	parse_options(Rest, Config#config{ debug = B });

parse_options([{<<"rating_rates">>, {RatesConfig}} | Rest], Config) when is_list(RatesConfig) ->
	{_,T} = Config#config.wildfire,
	case parse_rates(RatesConfig, #{}) of
		{ok, Rates} ->
			parse_options(Rest, Config#config{ wildfire = {Rates,T} });
		Error ->
			{error, bad_rates, Error}
	end;

parse_options([{<<"rating_triggers">>, {TriggersConfig}} | Rest], Config) when is_list(TriggersConfig) ->
	{R,_} = Config#config.wildfire,
	case parse_triggers(TriggersConfig, #{}) of
		{ok, Triggers} ->
			parse_options(Rest, Config#config{ wildfire = {R,Triggers} });
		Error ->
			{error, bad_triggers, Error}
	end;

parse_options([Opt | _], _) ->
	{error, unknown, Opt};
parse_options([], Config) ->
	{ok, Config}.

safe_map(Fun, List) ->
	try
		{ok, lists:map(Fun, List)}
	catch
		_:_ -> error
	end.

parse_peers([Peer | Rest], ParsedPeers) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ParsedPeer} -> parse_peers(Rest, [ParsedPeer | ParsedPeers]);
		{error, _} -> error
	end;
parse_peers([], ParsedPeers) ->
	{ok, lists:reverse(ParsedPeers)}.

parse_webhooks([{WebhookConfig} | Rest], ParsedWebhookConfigs) when is_list(WebhookConfig) ->
	case parse_webhook(WebhookConfig, #config_webhook{}) of
		{ok, ParsedWebhook} -> parse_webhooks(Rest, [ParsedWebhook | ParsedWebhookConfigs]);
		error -> error
	end;
parse_webhooks([_ | _], _) ->
	error;
parse_webhooks([], ParsedWebhookConfigs) ->
	{ok, lists:reverse(ParsedWebhookConfigs)}.

parse_webhook([{<<"events">>, Events} | Rest], Webhook) when is_list(Events) ->
	case parse_webhook_events(Events, []) of
		{ok, ParsedEvents} ->
			parse_webhook(Rest, Webhook#config_webhook{ events = ParsedEvents });
		error ->
			error
	end;
parse_webhook([{<<"events">>, _} | _], _) ->
	error;
parse_webhook([{<<"url">>, Url} | Rest], Webhook) when is_binary(Url) ->
	parse_webhook(Rest, Webhook#config_webhook{ url = Url });
parse_webhook([{<<"url">>, _} | _], _) ->
	error;
parse_webhook([{<<"headers">>, {Headers}} | Rest], Webhook) when is_list(Headers) ->
	parse_webhook(Rest, Webhook#config_webhook{ headers = Headers });
parse_webhook([{<<"headers">>, _} | _], _) ->
	error;
parse_webhook([], Webhook) ->
	{ok, Webhook}.

parse_webhook_events([Event | Rest], Events) ->
	case Event of
		<<"transaction">> -> parse_webhook_events(Rest, [transaction | Events]);
		<<"block">> -> parse_webhook_events(Rest, [block | Events]);
		_ -> error
	end;
parse_webhook_events([], Events) ->
	{ok, lists:reverse(Events)}.

parse_semaphores({[Semaphore | Semaphores]}, ParsedSemaphores) when is_tuple(Semaphore) ->
	parse_semaphores({Semaphores}, parse_semaphore(Semaphore, ParsedSemaphores));
parse_semaphores({[]}, ParsedSemaphores) ->
	{ok, ParsedSemaphores};
parse_semaphores(_, _) ->
	error.

parse_semaphore({Name, Number}, ParsedSemaphores)
		when is_binary(Name), is_number(Number) ->
	maps:put(binary_to_existing_atom(Name), Number, ParsedSemaphores);
parse_semaphore({Unknown, _N}, ParsedSemaphores) ->
	?LOG_WARNING([
		{event, configured_semaphore_bad_type},
		{semaphore, io_lib:format("~p", [Unknown])}
	]),
	ParsedSemaphores.

parse_rates([], R) ->
	?LOG_ERROR("RRRR ~p", [R]),
	{ok, R};
parse_rates([{<<"request_tx">>,N} |Rates], R) when is_number(N) ->
	parse_rates(Rates, maps:put({request, tx}, N, R));
parse_rates([{<<"request_block">>,N} |Rates], R) when is_number(N) ->
	parse_rates(Rates, maps:put({request, block}, N, R));
parse_rates([{<<"request_chunk">>,N} |Rates], R) when is_number(N) ->
	parse_rates(Rates, maps:put({request, chunk}, N, R));
parse_rates([{<<"push_tx">>,N} |Rates], R) when is_number(N) ->
	parse_rates(Rates, maps:put({push, tx}, N, R));
parse_rates([{<<"push_block">>,N} |Rates], R) when is_number(N) ->
	parse_rates(Rates, maps:put({push, block}, N, R));
parse_rates([{<<"response_tx">>,N} |Rates], R) when is_number(N) ->
	parse_rates(Rates, maps:put({response, tx}, N, R));
parse_rates([{<<"response_block">>,N} |Rates], R) when is_number(N) ->
	parse_rates(Rates, maps:put({response, block}, N, R));
parse_rates([{<<"response_chunk">>,N} |Rates], R) when is_number(N) ->
	parse_rates(Rates, maps:put({response, chunk}, N, R));
parse_rates([{<<"response">>,N} |Rates], R) when is_number(N) ->
	parse_rates(Rates, maps:put({response, any}, N, R));
parse_rates([{<<"request_malformed">>,N} |Rates], R) when is_number(N) ->
	parse_rates(Rates, maps:put({request, malformed}, N, R));
parse_rates([{<<"response_malformed">>,N} |Rates], R) when is_number(N) ->
	parse_rates(Rates, maps:put({response, malformed}, N, R));
parse_rates([{<<"response_connect_timeout">>,N} |Rates], R) when is_number(N) ->
	parse_rates(Rates, maps:put({response, connect_timeout}, N, R));
parse_rates([{<<"response_request_timeout">>,N} |Rates], R) when is_number(N) ->
	parse_rates(Rates, maps:put({response, request_timeout}, N, R));
parse_rates([{<<"response_not_found">>,N} |Rates], R) when is_number(N) ->
	parse_rates(Rates, maps:put({response, not_found}, N, R));
parse_rates([{<<"push_malformed">>,N} |Rates], R) when is_number(N) ->
	parse_rates(Rates, maps:put({push, malformed}, N, R));
parse_rates([{<<"attack">>,N} |Rates], R) when is_number(N) ->
	parse_rates(Rates, maps:put({attack, any}, N, R));
parse_rates([Unknown|Rates], R) ->
	?LOG_ERROR("unknown rate option ~p", [Unknown]),
	parse_rates(Rates, R).

parse_triggers([], T) ->
	?LOG_ERROR("TTTT ~p", [T]),
	{ok, T};
parse_triggers([{<<"request_tx">>,{Trigger}} |Triggers], T) when is_list(Trigger) ->
	Opts = {undefined,undefined,undefined,undefined},
	case parse_trigger(Trigger,Opts) of
		{ok, {I,P,Tr,V}} ->
			parse_triggers(Triggers, maps:put({request, tx}, {I,P,Tr,V}, T));
		_ ->
			error
	end;
parse_triggers([{<<"request_block">>,{Trigger}} |Triggers], T) when is_list(Trigger) ->
	Opts = {undefined,undefined,undefined,undefined},
	case parse_trigger(Trigger,Opts) of
		{ok, {I,P,Tr,V}} ->
			parse_triggers(Triggers, maps:put({request, block}, {I,P,Tr,V}, T));
		_ ->
			error
	end;
parse_triggers([{<<"request_chunk">>,{Trigger}} |Triggers], T) when is_list(Trigger) ->
	Opts = {undefined,undefined,undefined,undefined},
	case parse_trigger(Trigger,Opts) of
		{ok, {I,P,Tr,V}} ->
			parse_triggers(Triggers, maps:put({request, chunk}, {I,P,Tr,V}, T));
		_ ->
			error
	end;
parse_triggers([{<<"push_tx">>,{Trigger}} |Triggers], T) when is_list(Trigger) ->
	Opts = {undefined,undefined,undefined,undefined},
	case parse_trigger(Trigger,Opts) of
		{ok, {I,P,Tr,V}} ->
			parse_triggers(Triggers, maps:put({push, tx}, {I,P,Tr,V}, T));
		_ ->
			error
	end;
parse_triggers([{<<"push_block">>,{Trigger}} |Triggers], T) when is_list(Trigger) ->
	Opts = {undefined,undefined,undefined,undefined},
	case parse_trigger(Trigger,Opts) of
		{ok, {I,P,Tr,V}} ->
			parse_triggers(Triggers, maps:put({push, block}, {I,P,Tr,V}, T));
		_ ->
			error
	end;
parse_triggers([{<<"response_tx">>,{Trigger}} |Triggers], T) when is_list(Trigger) ->
	Opts = {undefined,undefined,undefined,undefined},
	case parse_trigger(Trigger,Opts) of
		{ok, {I,P,Tr,V}} ->
			parse_triggers(Triggers, maps:put({response, tx}, {I,P,Tr,V}, T));
		_ ->
			error
	end;
parse_triggers([{<<"response_block">>,{Trigger}} |Triggers], T) when is_list(Trigger) ->
	Opts = {undefined,undefined,undefined,undefined},
	case parse_trigger(Trigger,Opts) of
		{ok, {I,P,Tr,V}} ->
			parse_triggers(Triggers, maps:put({response, block}, {I,P,Tr,V}, T));
		_ ->
			error
	end;
parse_triggers([{<<"response_chunk">>,{Trigger}} |Triggers], T) when is_list(Trigger) ->
	Opts = {undefined,undefined,undefined,undefined},
	case parse_trigger(Trigger,Opts) of
		{ok, {I,P,Tr,V}} ->
			parse_triggers(Triggers, maps:put({response, chunk}, {I,P,Tr,V}, T));
		_ ->
			error
	end;
parse_triggers([{<<"response">>,{Trigger}} |Triggers], T) when is_list(Trigger) ->
	Opts = {undefined,undefined,undefined,undefined},
	case parse_trigger(Trigger,Opts) of
		{ok, {I,P,Tr,V}} ->
			parse_triggers(Triggers, maps:put({response, any}, {I,P,Tr,V}, T));
		_ ->
			error
	end;
parse_triggers([{<<"request_malformed">>,{Trigger}} |Triggers], T) when is_list(Trigger) ->
	Opts = {undefined,undefined,undefined,undefined},
	case parse_trigger(Trigger,Opts) of
		{ok, {I,P,Tr,V}} ->
			parse_triggers(Triggers, maps:put({request, malformed}, {I,P,Tr,V}, T));
		_ ->
			error
	end;
parse_triggers([{<<"response_malformed">>,{Trigger}} |Triggers], T) when is_list(Trigger) ->
	Opts = {undefined,undefined,undefined,undefined},
	case parse_trigger(Trigger,Opts) of
		{ok, {I,P,Tr,V}} ->
			parse_triggers(Triggers, maps:put({response, malformed}, {I,P,Tr,V}, T));
		_ ->
			error
	end;
parse_triggers([{<<"response_connect_timeout">>,{Trigger}} |Triggers], T) when is_list(Trigger) ->
	Opts = {undefined,undefined,undefined,undefined},
	case parse_trigger(Trigger,Opts) of
		{ok, {I,P,Tr,V}} ->
			parse_triggers(Triggers, maps:put({response, connect_timeout}, {I,P,Tr,V}, T));
		_ ->
			error
	end;
parse_triggers([{<<"response_request_timeout">>,{Trigger}} |Triggers], T) when is_list(Trigger) ->
	Opts = {undefined,undefined,undefined,undefined},
	case parse_trigger(Trigger,Opts) of
		{ok, {I,P,Tr,V}} ->
			parse_triggers(Triggers, maps:put({response, request_timeout}, {I,P,Tr,V}, T));
		_ ->
			error
	end;
parse_triggers([{<<"response_not_found">>,{Trigger}} |Triggers], T) when is_list(Trigger) ->
	Opts = {undefined,undefined,undefined,undefined},
	case parse_trigger(Trigger,Opts) of
		{ok, {I,P,Tr,V}} ->
			parse_triggers(Triggers, maps:put({response, not_found}, {I,P,Tr,V}, T));
		_ ->
			error
	end;
parse_triggers([{<<"push_malformed">>,{Trigger}} |Triggers], T) when is_list(Trigger) ->
	Opts = {undefined,undefined,undefined,undefined},
	case parse_trigger(Trigger,Opts) of
		{ok, {I,P,Tr,V}} ->
			parse_triggers(Triggers, maps:put({push, malformed}, {I,P,Tr,V}, T));
		_ ->
			error
	end;
parse_triggers([{<<"attack">>,{Trigger}} |Triggers], T) when is_list(Trigger) ->
	Opts = {undefined,undefined,undefined,undefined},
	case parse_trigger(Trigger,Opts) of
		{ok, {I,P,Tr,V}} ->
			parse_triggers(Triggers, maps:put({attack, any}, {I,P,Tr,V}, T));
		_ ->
			error
	end;
parse_triggers([Unknown|Triggers], T) ->
	?LOG_ERROR("unknown trigger option ~p", [Unknown]),
	parse_triggers(Triggers, T).

parse_trigger([], {I,P,T,V}) when I == undefined; P == undefined; T == undefined; V == undefined ->
	?LOG_ERROR("incorrect trigger options"),
	error;
parse_trigger([], Opts) ->
	{ok, Opts};
parse_trigger([{<<"intensity">>, I}|Options], {_,Period,Trigger,Value}) when is_number(I), I > 0 ->
	parse_trigger(Options, {I, Period, Trigger, Value});
parse_trigger([{<<"period">>, P}|Options], {Intensity,_,Trigger,Value}) when is_number(P), P >= 0 ->
	parse_trigger(Options, {Intensity, P, Trigger, Value});
parse_trigger([{<<"trigger">>, <<"bonus">>}|Options], {Intensity,Period,_,Value}) ->
	parse_trigger(Options, {Intensity, Period, bonus, Value});
parse_trigger([{<<"trigger">>, <<"ban">>}|Options], {Intensity,Period,_,Value}) ->
	parse_trigger(Options, {Intensity, Period, ban, Value});
parse_trigger([{<<"trigger">>, <<"penalty">>}|Options], {Intensity,Period,_,Value}) ->
	parse_trigger(Options, {Intensity, Period, penalty, Value});
parse_trigger([{<<"value">>, V}|Options], {Intensity,Period,Trigger,_}) when is_number(V), V > 0 ->
	parse_trigger(Options, {Intensity, Period, Trigger, V});
parse_trigger([Option|_],_) ->
	?LOG_ERROR("incorrect trigger option: ~p", [Option]),
	error.



