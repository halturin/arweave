
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

-export([]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {
	joining = false,
	connected = false,
	blocks = []
}).

%%%===================================================================
%%% API
%%%===================================================================


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
	ar_events:subscribe(network),
	Connected = ar_network:is_connected(),
	gen_server:cast(?MODULE, join),
	{ok, #state{connected = Connected}}.

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
handle_cast(join, State) ->
	?LOG_ERROR("111111111111111 join..."),
	{ok, Config} = application:get_env(arweave, config),
	BI =
		case {Config#config.start_from_block_index, Config#config.init} of
			{false, false} ->
				not_joined;
			{true, _} ->
				case ar_storage:read_block_index() of
					{error, enoent} ->
						io:format(
							"~n~n\tBlock index file is not found. "
							"If you want to start from a block index copied "
							"from another node, place it in "
							"<data_dir>/hash_lists/last_block_index.json~n~n"
						),
						erlang:halt();
					BI2 ->
						BI2
				end;
			{false, true} ->
				Config2 = Config#config{ init = false },
				application:set_env(arweave, config, Config2),
				ar_weave:init(
					ar_util:genesis_wallets(),
					ar_retarget:switch_to_linear_diff(Config#config.diff),
					0,
					ar_storage:read_tx(ar_weave:read_v1_genesis_txs())
				)
		end,
	case {BI, Config#config.auto_join} of
		{not_joined, true} ->
			gen_server:cast(?MODULE, joining),
			{noreply, State#state{joining = true}};
		{[#block{} = GenesisB], true} ->
			BI = [ar_util:block_index_entry_from_block(GenesisB)],
			ar_node_state:init(BI),
			ar_events:send(node, {joined, BI, [GenesisB]}),
			{stop, normal, State};
		{BI, true} ->
			ar_node_state:init(BI),
			Blocks = read_recent_blocks(BI),
			ar_events:send(node, {joined, BI, Blocks}),
			{stop, normal, State};
		{_, false} ->
			{stop, normal, State}
	end;

handle_cast(joining, State) when State#state.connected == false ->
	?LOG_ERROR("111111111111111 joining (not connected) ..."),
	%% do nothing. waiting for the {event, network, connected}
	{noreply, State};

handle_cast(joining, State) when State#state.connected ->
	?LOG_ERROR("111111111111111 joining ..."),
	case ar_network:get_current_block_index() of
		error ->
			?LOG_ERROR("111111111111111 joining (next attempt) ... "),
			timer:send_after(?REJOIN_TIMEOUT, {'$gen_cast', joining}),
			{noreply, State};
		BI ->
			{Hash, _, _} = hd(BI),
			ar:console("Fetching current block... ~p~n", [{hash, ar_util:encode(Hash)}]),
			case ar_network:get_block(Hash) of
				{error, _} ->
					ar:console(
						"Did not manage to fetch current block from any of the peers. Will retry later.~n"
					),
					timer:send_after(?REJOIN_TIMEOUT, {'$gen_cast', joining}),
					{noreply, State};
				B ->
					ar:console("Joining the Arweave network...~n"),
					%% Get a block, and its 2 * ?MAX_TX_ANCHOR_DEPTH previous blocks.
					%% If the block list is shorter than 2 * ?MAX_TX_ANCHOR_DEPTH, simply
					%% get all existing blocks.
					%%
					%% The node needs 2 * ?MAX_TX_ANCHOR_DEPTH block anchors so that it
					%% can validate transactions even if it enters a ?MAX_TX_ANCHOR_DEPTH-deep
					%% fork recovery (which is the deepest fork recovery possible) immediately after
					%% joining the network.
					Behind = 2 * ?MAX_TX_ANCHOR_DEPTH,
					gen_server:cast(?MODULE, {trail_blocks, Behind, B, BI}),
					{noreply, State#state{blocks = []}}
			end
	end;

handle_cast({trail_blocks, Behind, B, BI}, State) when State#state.connected == false ->
	?LOG_ERROR("111111111111111 trailing (not connected) ... []"),
	Message = {trail_blocks, Behind, B, BI},
	erlang:send_after(?REJOIN_TIMEOUT, {'$gen_cast', Message}),
	{noreply, State};

handle_cast({trail_blocks, Behind, B, BI}, State) when Behind == 0; B#block.height == 0 ->
	?LOG_ERROR("111111111111111 trailing ... done "),
	ar_arql_db:populate_db(?BI_TO_BHL(BI)),
	ar_node_state:init(BI),
	ar_events:send(node, {joined, BI, State#state.blocks}),
	ar:console("Joined the Arweave network successfully.~n");

handle_cast({trail_blocks, Behind, B, BI}, State) ->
	?LOG_ERROR("111111111111111 trailing ... ~p", [Behind]),
	PreviousB = ar_network:get_block(B#block.previous_block),
	case ?IS_BLOCK(PreviousB) of
		true ->
			SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(B#block.txs),
			Blocks = [B#block{ size_tagged_txs = SizeTaggedTXs } | State#state.blocks],
			gen_server:cast(?MODULE, {trail_blocks, Behind-1, PreviousB, BI}),
			{noreply, State#state{blocks = Blocks}};
		false ->
			?LOG_INFO(
				[{event, could_not_retrieve_joining_block}]
			),
			Message = {trail_blocks, Behind, B, BI},
			erlang:send_after(?REJOIN_TIMEOUT, {'$gen_cast', Message}),
			{noreply, State}
	end;

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
handle_info({event, network, connected}, State) when State#state.joining ->
	gen_server:cast(?MODULE, joining),
	{noreply, State#state{connected = true}};

handle_info({event, network, connected}, State) ->
	{noreply, State#state{connected = true}};

handle_info({event, network, disconnected}, State) ->
	{noreply, State#state{connected = false}};

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
read_recent_blocks(not_joined) ->
	[];
read_recent_blocks(BI) ->
	read_recent_blocks2(lists:sublist(BI, 2 * ?MAX_TX_ANCHOR_DEPTH)).

read_recent_blocks2([]) ->
	[];
read_recent_blocks2([{BH, _, _} | BI]) ->
	B = ar_storage:read_block(BH),
	TXs = ar_storage:read_tx(B#block.txs),
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs),
	[B#block{ size_tagged_txs = SizeTaggedTXs, txs = TXs } | read_recent_blocks2(BI)].


