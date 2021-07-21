-module(ar_test_disk_cache).


-include_lib("common_test/include/ct.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-export([
	store_tx_block/1,
	reset_cache/1
]).

-define(CACHE_DIR, "cache/").

store_tx_block(Config) ->
	logger:set_module_level(ar_disk_cache, debug),
	logger:set_module_level(ar_http_iface_middleware, debug),
	{ok, AppConfig} = application:get_env(arweave, config),
	LimitMax = AppConfig#config.disk_cache_size * 1048576, % MB to Bytes,
	LimitMin = trunc(LimitMax * (100 - ?DISK_CACHE_CLEAN_PERCENT_MAX)/100),
	Path = filename:join(AppConfig#config.data_dir, ?CACHE_DIR),
	Wallet = ?config(wallet, Config),

	% get current height and state of the disk cache
	CurrentHeight = ar_node:get_height(),
	{TakenOnStart, FilesOnStart} = filelib:fold_files(
		Path,
		".*\\.json$",
		true,
		fun(F,{Total, Files}) ->
				{Total + filelib:file_size(F),
				[{filelib:last_modified(F), filelib:file_size(F), F}|Files]} end,
		{0, []}),

	% generate 10 blocks with 10 txs
	lists:foldl(fun(N, {Taken, Files}) ->
		% create a signed tx
		TX = ar_test_lib:sign_tx(Wallet,
								 #{
								   data => crypto:strong_rand_bytes(102400),
								   last_tx => ar_test_lib:get_tx_anchor()
								  }),
		% compute the tx file size
		TXData = ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX)),
		TXFileName = Path ++ "/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)) ++ ".json",
		Files1 = [{calendar:local_time(), byte_size(TXData), TXFileName} | Files],

		% post tx
		ar_test_lib:post_txs([TX]),

		Taken1 = Taken + byte_size(TXData),
		{Taken2, Files2} = check_cache(Taken1, LimitMax, LimitMin, Files1),

		ar_test_lib:mine(),
		Block = ar_test_lib:wait_for_block(N),

		% compute the block file size
		BlockData = ar_serialize:jsonify(ar_serialize:block_to_json_struct(Block)),
		BlockFileName = Path ++ "/block/" ++ binary_to_list(ar_util:encode(Block#block.indep_hash)) ++ ".json",
		Files3 = [{calendar:local_time(), byte_size(BlockData), BlockFileName} | Files2],

		% Check how disk cache handled new comes files.
		Taken3 = Taken2 + byte_size(BlockData),
		check_cache(Taken3, LimitMax, LimitMin, Files3)
	end, {TakenOnStart,FilesOnStart}, lists:seq(CurrentHeight+1, CurrentHeight + 10)),

	Config.

reset_cache(Config) ->
	{ok, AppConfig} = application:get_env(arweave, config),
	Path = filename:join(AppConfig#config.data_dir, ?CACHE_DIR),
	ok = ar_disk_cache:reset(),
	{0, []} = filelib:fold_files(
		Path,
		".*\\.json$",
		true,
		fun(F,{Total, Files}) ->
				{Total + filelib:file_size(F),
				[{filelib:last_modified(F), filelib:file_size(F), F}|Files]} end,
		{0, []}),
	logger:set_module_level(ar_disk_cache, info),
	Config.

%%%===================================================================
%%% Internal functions
%%%===================================================================
check_cache(Taken, LimitMax, LimitMin, Files) when Taken > LimitMax ->
	{ok, AppConfig} = application:get_env(arweave, config),
	Path = filename:join(AppConfig#config.data_dir, ?CACHE_DIR),
	% compute the expecting size that the disk cache must take after the cleaning up.
	CleanSize = Taken - LimitMin,
	{Files1, CleanedBytes} = delete_file(lists:sort(Files), CleanSize),
	Taken1 = Taken - (CleanSize - CleanedBytes),
	% compute disk case size
	DiskCacheTaken = filelib:fold_files(
					  Path,
					  ".*\\.json$",
					  true,
					  fun(F,Acc) -> filelib:file_size(F)+Acc end,
					  0),
	% must be equal
	DiskCacheTaken = Taken1,
	% check the disk cache
	% case 1: non existing data. just removed. (via module)
	[{_, _, RemovedPathFile} | _ ] = lists:sort(Files),
	RemovedFilename = filename:basename(RemovedPathFile, ".json"),
	case filename:basename(filename:dirname(RemovedPathFile)) of
		"tx" ->
			% should be unavailable in cache
			unavailable = ar_disk_cache:lookup_tx_filename(list_to_binary(RemovedFilename)),

			% but should be presented in the storage...
			% case 3: non existing data in the disk cache, but existing in the storage (via http)
			{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
				ar_http:req(#{
					method => get,
					peer => {127, 0, 0, 1, AppConfig#config.port},
					path => "/tx/" ++ RemovedFilename % tx hash
				});

		 "block" ->
			% should be unavailable in cache
			unavailable = ar_disk_cache:lookup_block_filename(list_to_binary(RemovedFilename)),
			% but should be presented in the storage
			% case 3: ...
			{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
				ar_http:req(#{
					method => get,
					peer => {127, 0, 0, 1, AppConfig#config.port},
					path => "/block/hash/" ++ RemovedFilename % block hash
				})
	end,
	% case 2: existing data (via module)
	[{_, _, ExistingPathFile} | _ ] = lists:sort(Files1),
	ExistingFilename = filename:basename(ExistingPathFile, ".json"),
	case filename:basename(filename:dirname(ExistingPathFile)) of
		"tx" ->
			% should return the same full path and filename
			ExistingPathFile = ar_disk_cache:lookup_tx_filename(list_to_binary(ExistingFilename));
		 "block" ->
			% should return the same full path and filename
			ExistingPathFile = ar_disk_cache:lookup_block_filename(list_to_binary(ExistingFilename))
	end,
	{Taken1, Files1};
check_cache(Size, _LimitMax, _LimitMin, Files) ->
	{Size, Files}.

delete_file([], CleanSize) ->
	{[], CleanSize};
delete_file(Files, CleanSize) when CleanSize < 0 ->
	{Files, CleanSize};
delete_file([{_DateTime, Size, _FileName} | Files], CleanSize) ->
	delete_file(Files, CleanSize - Size).
