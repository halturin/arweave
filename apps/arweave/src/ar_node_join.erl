
%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_node_join).

-behaviour(gen_server).

-export([
	start_link/0
]).

-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-export([
	randomx_state_by_height/1,
	hash/2
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {
}).

%%%===================================================================
%%% API
%%%===================================================================


hash(Height, Data) ->
	case randomx_state_by_height(Height) of
		{state, {fast, FastState}} ->
			ar_mine_randomx:hash_fast(FastState, Data);
		{state, {light, LightState}} ->
			ar_mine_randomx:hash_light(LightState, Data);
		{key, Key} ->
			LightState = ar_mine_randomx:init_light(Key),
			ar_mine_randomx:hash_light(LightState, Data)
	end.

randomx_state_by_height(Height) when is_integer(Height) andalso Height >= 0 ->
	case gen_server:call(?MODULE, {get_state_by_height, Height}) of
		{state_not_found, key_not_found} ->
			SwapHeight = 1, %swap_height(Height),
			{ok, Key} = randomx_key(SwapHeight),
			{key, Key};
		{state_by_height, {state_not_found, Key}} ->
			{key, Key};
		{state_by_height, {ok, NodeState}} ->
			{state, NodeState}
	end.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%					   {ok, State, Timeout} |
%%					   ignore |
%%					   {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
	gen_server:cast(?MODULE, poll_new_blocks),
	{ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%									 {reply, Reply, State} |
%%									 {reply, Reply, State, Timeout} |
%%									 {noreply, State} |
%%									 {noreply, State, Timeout} |
%%									 {stop, Reason, Reply, State} |
%%									 {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(Request, _From, State) ->
	?LOG_ERROR("unhandled call: ~p", [Request]),
	{reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%									{noreply, State, Timeout} |
%%									{stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, State}.

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
handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
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
terminate(_Reason, _State) ->
	?LOG_INFO([{event, ar_node_join}, {module, ?MODULE}]),
	ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Return the key used in RandomX by key swap height. The key is the
%% dependent hash from the block at the previous swap height. If RandomX is used
%% already by the first ?RANDOMX_KEY_SWAP_FREQ blocks, then a hardcoded key is
%% used since there is no old enough block to fetch the key from.
randomx_key(SwapHeight) when SwapHeight < ?RANDOMX_KEY_SWAP_FREQ ->
	{ok, <<"Arweave Genesis RandomX Key">>};
randomx_key(SwapHeight) ->
	KeyBlockHeight = SwapHeight - ?RANDOMX_KEY_SWAP_FREQ,
	case get_block(KeyBlockHeight) of
		{ok, KeyB} ->
			Key = KeyB#block.hash,
			whereis(?MODULE) ! {cache_randomx_key, SwapHeight, Key},
			{ok, Key};
		unavailable ->
			unavailable
	end.
randomx_key(SwapHeight, _, _) when SwapHeight < ?RANDOMX_KEY_SWAP_FREQ ->
	randomx_key(SwapHeight);
randomx_key(SwapHeight, BI, Peers) ->
	KeyBlockHeight = SwapHeight - ?RANDOMX_KEY_SWAP_FREQ,
	case get_block(KeyBlockHeight, BI, Peers) of
		{ok, KeyB} ->
			{ok, KeyB#block.hash};
		unavailable ->
			unavailable
	end.

get_block(Height) ->
	case ar_node:get_block_index() of
		[] -> unavailable;
		BI ->
			{BH, _, _} = lists:nth(Height + 1, lists:reverse(BI)),
			get_block(BH, BI)
	end.

get_block(BH, BI) ->
	Peers = ar_bridge:get_remote_peers(),
	get_block(BH, BI, Peers).

get_block(Height, BI, Peers) when is_integer(Height) ->
	{BH, _, _} = lists:nth(Height + 1, lists:reverse(BI)),
	get_block(BH, BI, Peers);
get_block(BH, BI, Peers) ->
	case ar_http_iface_client:get_block(Peers, BH) of
		unavailable ->
			unavailable;
		B ->
			case ar_weave:indep_hash(B) of
				BH ->
					SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(B#block.txs),
					ar_data_sync:add_block(B, SizeTaggedTXs),
					ar_header_sync:add_block(B),
					{ok, B};
				InvalidBH ->
					?LOG_WARNING([
						{event, ar_randomx_state_got_invalid_block},
						{requested_block_hash, ar_util:encode(BH)},
						{received_block_hash, ar_util:encode(InvalidBH)}
					]),
					get_block(BH, BI)
			end
	end.
