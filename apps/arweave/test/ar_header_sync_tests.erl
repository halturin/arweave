-module(ar_header_sync_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_header_sync.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

-import(ar_test_node, [
	start/1,
	join_on_master/0,
	slave_start/1,
	connect_to_slave/0,
	slave_call/3,
	sign_tx/3, assert_post_tx_to_master/1,
	wait_until_height/1, assert_slave_wait_until_height/1,
	read_block_when_stored/1
]).

syncs_headers_test_() ->
	{timeout, 120, fun test_syncs_headers/0}.

test_syncs_headers() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(200), <<>>}]),
	{_Master, _} = start(B0),
	slave_start(B0),
	post_random_blocks(Wallet, 2 * ?MAX_TX_ANCHOR_DEPTH + 5, B0),
	join_on_master(),
	connect_to_slave(),
	BI = assert_slave_wait_until_height(2 * ?MAX_TX_ANCHOR_DEPTH + 5),
	lists:foreach(
		fun(Height) ->
			{ok, B} = ar_util:do_until(
				fun() ->
					case slave_call(ar_storage, read_block, [Height, BI]) of
						unavailable ->
							unavailable;
						B2 ->
							{ok, B2}
					end
				end,
				200,
				30000
			),
			MasterB = ar_storage:read_block(Height, ar_node:get_block_index()),
			?assertEqual(B, MasterB),
			TXs = slave_call(ar_storage, read_tx, [B#block.txs]),
			MasterTXs = ar_storage:read_tx(B#block.txs),
			?assertEqual(TXs, MasterTXs)
		end,
		lists:reverse(lists:seq(0, 5))
	),
	B1 = ar_storage:read_block(1, BI),
	ar_meta_db:put(disk_space, ?DISK_HEADERS_BUFFER_SIZE + 100000),
	ar_meta_db:put(
		used_space,
		?DISK_HEADERS_BUFFER_SIZE + 100000 - ?DISK_HEADERS_CLEANUP_THRESHOLD + 1
	),
	true = ar_util:do_until(
		fun() ->
			case ar_storage:read_block(0, BI) of
				unavailable ->
					true;
				_ ->
					false
			end
		end,
		200,
		?CHECK_AFTER_SYNCED_INTERVAL_MS * 2
	),
	?assertEqual([unavailable || _ <- B0#block.txs], ar_storage:read_tx(B0#block.txs)),
	?assertMatch(#block{}, ar_storage:read_block(1, BI)),
	ar_meta_db:put(disk_space, ?DISK_HEADERS_BUFFER_SIZE),
	ar_meta_db:put(used_space, ?DISK_HEADERS_BUFFER_SIZE - ?DISK_HEADERS_CLEANUP_THRESHOLD + 1),
	ar_node:mine(),
	[{H, _, _} | _] = wait_until_height(length(BI)),
	#block{} = read_block_when_stored(H),
	true = ar_util:do_until(
		fun() ->
			case ar_storage:read_block(1, BI) of
				unavailable ->
					true;
				_ ->
					false
			end
		end,
		200,
		?CHECK_AFTER_SYNCED_INTERVAL_MS * 2
	),
	?assertEqual([unavailable || _ <- B1#block.txs], ar_storage:read_tx(B1#block.txs)).

post_random_blocks(Wallet, TargetHeight, B0) ->
	lists:foldl(
		fun(Height, Anchor) ->
			TXs =
				lists:foldl(
					fun(_, Acc) ->
						case rand:uniform(2) == 1 of
							true ->
								TX = sign_tx(master, Wallet, #{ last_tx => Anchor }),
								assert_post_tx_to_master(TX),
								[TX | Acc];
							false ->
								Acc
						end
					end,
					[],
					lists:seq(1, 2)
				),
			ar_node:mine(),
			[{H, _, _} | _] = wait_until_height(Height),
			?assertEqual(length(TXs), length((read_block_when_stored(H))#block.txs)),
			H
		end,
		B0#block.indep_hash,
		lists:seq(1, TargetHeight)
	).
