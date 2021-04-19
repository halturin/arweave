-module(ar_network_http_client).

% This module implements HTTP transport (client side) for the arweave network
% Every callback should have arity 2: Peer, Options
%   Peer: {A,B,C,D,Port}
%   Options: term
% and return
%   {ok, Value} - on success
%   not_found   - if requested information is not present on the peer
%   _           - any other result will be treated as an error
%
-export([
	get_time/2,
	get_peers/2,
	get_info/2,
	get_block_index/2,
	get_block/2,
	get_tx/2,
	get_wallet_list/2,
	get_wallet_list_chunk/2,
	get_chunk/2,
	get_sync_record/2,
	post_tx/2,
	post_block/2
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_wallets.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").



%% @doc Retreive the current universal time as claimed by a foreign node.
get_time(Peer, _) ->
	case catch
		ar_http:req(#{
					method => get,
					peer => Peer,
					path => "/time",
					timeout => 5000,
					headers => p2p_headers()
		})
	of
		{ok, {{<<"200">>, _}, _, Body, Start, End}} ->
			Time = binary_to_integer(Body),
			RequestTime = ceil((End - Start) / 1000000),
			%% The timestamp returned by the HTTP daemon is floored second precision. Thus the
			%% upper bound is increased by 1.
			{ok, {Time - RequestTime, Time + RequestTime + 1}};
		_ ->
			error
	end.


%% @doc Return a list of parsed peer IPs for a remote server.
get_peers(Peer, _) ->
	case catch
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/peers",
			headers => p2p_headers()
		})
	of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			PeerArray = ar_serialize:dejsonify(Body),
			{ok, lists:map(fun ar_util:parse_peer/1, PeerArray)};
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
			not_found;
		_ ->
			error
	end.

get_info(Peer, _) ->
	case catch
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/info",
			headers => p2p_headers(),
			connect_timeout => 500,
			timeout => 2 * 1000
		})
	of
		{ok, {{<<"200">>, _}, _, JSON, _, _}} ->
			{ok, process_get_info_json(JSON)};
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
			not_found;
		A ->
			?LOG_ERROR("DBG get_info ~p", [A]),
			error
	end.

%% @doc Get a block hash list (by its hash) from the external peer.
get_block_index(Peer, _) ->
	?LOG_ERROR("DBG get_block_index enter ~p", [Peer]),
	case catch
		 ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/block_index",
			timeout => 120 * 1000,
			headers => p2p_headers()
		})
	of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			{ok, ar_serialize:json_struct_to_block_index(ar_serialize:dejsonify(Body))};
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
			not_found;
		A ->
			?LOG_ERROR("DBG get_block_index ~p", [A]),
			error
	end.

get_block(Peer, H) ->
	?LOG_ERROR("DBG get_block enter ~p", [Peer]),
	case catch
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => prepare_block_id(H),
			headers => p2p_headers(),
			connect_timeout => 500,
			timeout => 30 * 1000,
			limit => ?MAX_BODY_SIZE
		})
	of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			{ok, ar_serialize:json_struct_to_block(Body)};
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
			not_found;
		A ->
			?LOG_ERROR("DBG get_block error ~p", [A]),
			error
	end.

get_tx(Peer, TXID) ->
	?LOG_ERROR("DBG get_tx ~p enter ~p", [TXID, Peer]),
	case catch
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)),
			headers => p2p_headers(),
			connect_timeout => 500,
			timeout => 60 * 1000,
			limit => ?MAX_BODY_SIZE
		})
	of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			case catch ar_serialize:json_struct_to_tx(Body) of
				TX when is_record(TX, tx) ->
					case ar_tx:verify_tx_id(TXID, TX) of
						false ->
							?LOG_WARNING([
								{event, peer_served_invalid_tx},
								{peer, ar_util:format_peer(Peer)},
								{tx, ar_util:encode(TXID)}
							]),
							error;
						true ->
							{ok, TX}
					end;
				_ -> not_found
			end;
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
			not_found;
		{ok, {{<<"410">>, _}, _, _, _, _}} ->
			not_found;
		_ ->
			error
	end.



%% @doc Get a bunch of wallets by the given root hash from external peers.
get_wallet_list_chunk(Peer, {H, Cursor}) ->
	?LOG_ERROR("DBG get_wallet_list_chunk ~p enter ~p", [{H, Cursor}, Peer]),
	BasePath = "/wallet_list/" ++ binary_to_list(ar_util:encode(H)),
	Path =
		case Cursor of
			start ->
				BasePath;
			_ ->
				BasePath ++ "/" ++ binary_to_list(ar_util:encode(Cursor))
		end,
	case catch
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => Path,
			headers => p2p_headers(),
			limit => ?MAX_SERIALIZED_WALLET_LIST_CHUNK_SIZE,
			timeout => 10 * 1000,
			connect_timeout => 1000
		})
	of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			case ar_serialize:etf_to_wallet_chunk_response(Body) of
				{ok, #{ next_cursor := NextCursor, wallets := Wallets }} ->
					{ok, {NextCursor, Wallets}};
				DeserializationResult ->
					?LOG_ERROR([
						{event, got_unexpected_wallet_list_chunk_deserialization_result},
						{deserialization_result, DeserializationResult}
					]),
					unavailable
			end;
		_ ->
			not_found
	end;
get_wallet_list_chunk(Peer, H) ->
	get_wallet_list_chunk(Peer, {H, start}).

%% @doc Get a wallet list by the given block hash from external peers.
get_wallet_list(Peer, H) ->
	?LOG_ERROR("DBG get_wallet_list ~p enter ~p", [H, Peer]),
	case catch
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/block/hash/" ++ binary_to_list(ar_util:encode(H)) ++ "/wallet_list",
			headers => p2p_headers()
		})
	 of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			{ok, ar_serialize:json_struct_to_wallet_list(Body)};
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
			not_found;
		_ ->
			unavailable
	end.

get_sync_record(Peer, _) ->
	case catch
		ar_http:req(#{
			peer => Peer,
			method => get,
			path => "/data_sync_record",
			timeout => 5 * 1000,
			connect_timeout => 500,
			limit => ?MAX_ETF_SYNC_RECORD_SIZE,
			headers => [{<<"Content-Type">>, <<"application/etf">>} | p2p_headers()]
	})
	of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			ar_intervals:safe_from_etf(Body);
		_ ->
			unavailable
	end.

get_chunk(Peer, Offset) ->
	case catch
		ar_http:req(#{
			peer => Peer,
			method => get,
			path => "/chunk/" ++ integer_to_binary(Offset),
			timeout => 30 * 1000,
			connect_timeout => 500,
			limit => ?MAX_SERIALIZED_CHUNK_PROOF_SIZE,
			headers => p2p_headers()
	})
	of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			ar_serialize:json_map_to_chunk_proof(jiffy:decode(Body, [return_maps]));
		_ ->
			unavailable
	end.

post_tx(Peer, TX) ->
	TXSize = byte_size(TX#tx.data),
	case catch
		ar_http:req(#{
			method => post,
			peer => Peer,
			path => "/tx",
			headers => p2p_headers() ++ [{<<"arweave-tx-id">>, ar_util:encode(TX#tx.id)}],
			body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX)),
			connect_timeout => 500,
			timeout => max(3, min(60, TXSize * 8 div ?TX_PROPAGATION_BITS_PER_SECOND)) * 1000
	})
	of
		{ok, _} ->
			{ok, sent};
		_ ->
			error
	end.

post_block(Peer, {Block, BDS}) ->
	{BlockProps} = ar_serialize:block_to_json_struct(Block),
	BlockShadowProps =
		case Block#block.height >= ar_fork:height_2_4() of
			true ->
				BlockProps;
			false ->
				case Block#block.hash_list of
					unset ->
						BlockProps;
					_ ->
						ShortHashList =
							lists:map(
								fun ar_util:encode/1,
								lists:sublist(Block#block.hash_list, ?STORE_BLOCKS_BEHIND_CURRENT)
							),
						[{<<"hash_list">>, ShortHashList} | BlockProps]
				end
		end,
	PostProps = [
		{<<"new_block">>, {BlockShadowProps}},
		%% Add the P2P port field to be backwards compatible with nodes
		%% running the old version of the P2P port feature.
		{<<"port">>, ?DEFAULT_HTTP_IFACE_PORT},
		{<<"block_data_segment">>, ar_util:encode(BDS)}
	],
	case catch
		ar_http:req(#{
			method => post,
			peer => Peer,
			path => "/block",
			headers =>
				p2p_headers() ++ [{<<"arweave-block-hash">>, ar_util:encode(Block#block.indep_hash)}],
			body => ar_serialize:jsonify({PostProps}),
			timeout => 3 * 1000
		})
	of
		{ok, _} ->
			{ok, sent};
		_ ->
			error
	end.


%%
%% private functions
%%

p2p_headers() ->
	{ok, Config} = application:get_env(arweave, config),
	[{<<"x-p2p-port">>, integer_to_binary(Config#config.port)}].

process_get_info_json(JSON) ->
	case ar_serialize:json_decode(JSON) of
		{ok, {Props}} ->
			process_get_info(Props);
		{error, _} ->
			info_unavailable
	end.

process_get_info(Props) ->
	Keys = [<<"network">>, <<"version">>, <<"height">>, <<"blocks">>, <<"peers">>],
	case safe_get_vals(Keys, Props) of
		error ->
			info_unavailable;
		{ok, [NetworkName, ClientVersion, Height, Blocks, Peers]} ->
			ReleaseNumber =
				case lists:keyfind(<<"release">>, 1, Props) of
					false -> 0;
					R -> R
				end,
			[
				{name, NetworkName},
				{version, ClientVersion},
				{height, Height},
				{blocks, Blocks},
				{peers, Peers},
				{release, ReleaseNumber}
			]
	end.

safe_get_vals(Keys, Props) ->
	case lists:foldl(fun
			(_, error) -> error;
			(Key, Acc) ->
				case lists:keyfind(Key, 1, Props) of
					{_, Val} -> [Val | Acc];
					_		 -> error
				end
			end, [], Keys) of
		error -> error;
		Vals  -> {ok, lists:reverse(Vals)}
	end.

%% @doc Generate an appropriate URL for a block by its identifier.
prepare_block_id({ID, _, _}) ->
	prepare_block_id(ID);
prepare_block_id(ID) when is_binary(ID) ->
	"/block/hash/" ++ binary_to_list(ar_util:encode(ID));
prepare_block_id(ID) when is_integer(ID) ->
	"/block/height/" ++ integer_to_list(ID).


