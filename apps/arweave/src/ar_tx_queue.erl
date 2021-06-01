-module(ar_tx_queue).

-behaviour(gen_server).

%% API
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
	set_pause/1,
	show_queue/0,
	set_max_data_size/1,
	set_max_header_size/1,
	set_num_peers_tx_broadcast/1,
	drop_tx/1,
	utility/1
]).


-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

%% Internal state definition.
-record(state, {
	tx_queue = gb_sets:new(),
	max_header_size = ?TX_QUEUE_HEADER_SIZE_LIMIT,
	max_data_size = ?TX_QUEUE_DATA_SIZE_LIMIT,
	header_size = 0,
	data_size = 0,
	paused = false,
	num_peers_tx_broadcast = ?NUM_PEERS_TX_BROADCAST
}).


%%%===================================================================
%%% API
%%%===================================================================
set_pause(Paused) ->
	gen_server:call(?MODULE, {set_pause, Paused}).

show_queue() ->
	gen_server:call(?MODULE, show_queue).

set_max_data_size(Bytes) ->
	gen_server:call(?MODULE, {set_max_data_size, Bytes}).

set_max_header_size(Bytes) ->
	gen_server:call(?MODULE, {set_max_header_size, Bytes}).

set_num_peers_tx_broadcast(N) when N > 0->
	gen_server:call(?MODULE, {set_num_peers_tx_broadcast, N}).

drop_tx(TX) ->
	gen_server:call(?MODULE, {drop_tx, TX}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
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
	ar_events:subscribe(tx),
	gen_server:cast(?MODULE, process_queue),
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
handle_call({set_pause, false}, _From, State) when State#state.paused == true ->
	gen_server:cast(?MODULE, process_queue),
	{reply, ok, State#state{paused = false}};
handle_call({set_pause, true}, _From, State) ->
	?LOG_DEBUG("TX queue is set on pause"),
	{reply, ok, State#state{paused = true}};

handle_call(show_queue, _From, State) ->
	Reply = show_queue(State#state.tx_queue),
	{reply, Reply, State};

handle_call({set_max_data_size, Bytes}, _From, State) ->
	{reply, ok, State#state{ max_data_size = Bytes }};

handle_call({set_max_header_size, Bytes}, _From, State) ->
	{reply, ok, State#state{ max_header_size = Bytes }};

handle_call({set_num_peers_tx_broadcast, N}, _From, State) ->
	{reply, ok, State#state{ num_peers_tx_broadcast = N}};

handle_call({drop_tx, TX}, _From, State) ->
	#state{
		tx_queue = Q,
		header_size = HeaderSize,
		data_size = DataSize
	} = State,
	{TXHeaderSize, TXDataSize} = tx_queue_size(TX),
	U = utility(TX),
	Item = {U, {TX, {TXHeaderSize, TXDataSize}}},
	case gb_sets:is_element(Item, Q) of
		true ->
			{reply, ok, State#state{
				tx_queue = gb_sets:del_element(Item, Q),
				header_size = HeaderSize - TXHeaderSize,
				data_size = DataSize - TXDataSize
			}};
		false ->
			{reply, not_found, State}
	end;

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
handle_cast(_Msg, State) when State#state.paused == true ->
	?LOG_DEBUG("TX Queue is paused. Waiting..."),
	{noreply, State};

handle_cast(process_queue, State) when State#state.tx_queue == {0, nil} ->
	?LOG_DEBUG("TX Queue is empty. Waiting..."),
	{noreply, State};

handle_cast(process_queue, State) ->
	?LOG_DEBUG("TX Queue process..."),
	#state{
		tx_queue = Q,
		header_size = HeaderSize,
		data_size = DataSize,
		num_peers_tx_broadcast = NumPeersBroadcast
	} = State,
	{{_, {TX, {TXHeaderSize, TXDataSize}, FromPeerID}}, NewQ} = gb_sets:take_largest(Q),
	ExcludePeers = #{FromPeerID => true},
	BroadcastingStartedAt = erlang:timestamp(),
	?LOG_DEBUG("TX Queue broadcast TXID ~p (exclude ~p)", [ar_util:encode(TX#tx.id), ExcludePeers]),
	ar_network:broadcast(tx, TX, {ExcludePeers, NumPeersBroadcast}),
	BroadcastingTimeUs = timer:now_diff(erlang:timestamp(), BroadcastingStartedAt),
	record_propagation_rate(tx_propagated_size(TX), BroadcastingTimeUs),
	record_queue_size(gb_sets:size(NewQ)),
	% compute the delay we should wait before the sending this TX to the mining pool
	Delay = ar_node_utils:calculate_delay(tx_propagated_size(TX)),
	erlang:send_after(Delay, ?MODULE, {'$gen_cast', {send_to_mining_pool, TX}}),
	NewState = State#state{
		tx_queue = NewQ,
		header_size = HeaderSize - TXHeaderSize,
		data_size = DataSize - TXDataSize
	},
	gen_server:cast(?MODULE, process_queue),
	{noreply, NewState};

handle_cast({send_to_mining_pool, TX}, State) ->
	ar_events:send(tx, {mine, TX}),
	{noreply, State};

handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {message, Msg}]),
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
handle_info({event, tx, {new, TX, FromPeerID}}, State) ->
	#state{
		tx_queue = Q,
		max_header_size = MaxHeaderSize,
		max_data_size = MaxDataSize,
		header_size = HeaderSize,
		data_size = DataSize
	} = State,
	?LOG_DEBUG("TX Queue new tx received ~p from ~p", [TX#tx.id, FromPeerID]),
	{TXHeaderSize, TXDataSize} = tx_queue_size(TX),
	U = utility(TX),
	{NewQ, {NewHeaderSize, NewDataSize}, DroppedTXs} =
		maybe_drop(
			gb_sets:add_element({U, {TX, {TXHeaderSize, TXDataSize}, FromPeerID}}, Q),
			{HeaderSize + TXHeaderSize, DataSize + TXDataSize},
			{MaxHeaderSize, MaxDataSize}
		),
	case DroppedTXs of
		[] ->
			noop;
		_ ->
			DroppedIDs = lists:map(
				fun(DroppedTX) ->
					case TX#tx.format of
						2 ->
							ar_data_sync:maybe_drop_data_root_from_disk_pool(
								DroppedTX#tx.data_root,
								DroppedTX#tx.data_size,
								DroppedTX#tx.id
							);
						_ ->
							nothing_to_drop_from_disk_pool
					end,
					ar_util:encode(DroppedTX#tx.id)
				end,
				DroppedTXs
			),
			?LOG_INFO([
				{event, drop_txs_from_queue},
				{dropped_txs, DroppedIDs}
			]),
			ar_events:send(tx, {drop, DroppedTXs})
	end,
	NewState = State#state{
		tx_queue = NewQ,
		header_size = NewHeaderSize,
		data_size = NewDataSize
	},
	gen_server:cast(?MODULE, process_queue),
	{noreply, NewState};

handle_info({event, tx, {_, _TX, _FromPeerID}}, State) ->
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {info, Info}]),
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
terminate(Reason, _State) ->
	?LOG_INFO([{event, ar_tx_queue_terminated}, {reason, Reason}]),
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
maybe_drop(Q, Size, MaxSize) ->
	maybe_drop(Q, Size, MaxSize, []).

maybe_drop(Q, {HeaderSize, DataSize} = Size, {MaxHeaderSize, MaxDataSize} = MaxSize, DroppedTXs) ->
	case HeaderSize > MaxHeaderSize orelse DataSize > MaxDataSize of
		true ->
			{{_, {TX, {DroppedHeaderSize, DroppedDataSize}, _FromPeerID}}, NewQ} = gb_sets:take_smallest(Q),
			maybe_drop(
				NewQ,
				{HeaderSize - DroppedHeaderSize, DataSize - DroppedDataSize},
				MaxSize,
				[TX | DroppedTXs]
			);
		false ->
			{Q, Size, lists:filter(fun(TX) -> TX /= none end, DroppedTXs)}
	end.

show_queue(Q) ->
	gb_sets:fold(
		fun({_, {TX, _, _}}, Acc) ->
			[{ar_util:encode(TX#tx.id), TX#tx.reward, TX#tx.data_size} | Acc]
		end,
		[],
		Q
	).

utility(TX = #tx { data_size = DataSize }) ->
	utility(TX, ?TX_SIZE_BASE + DataSize).

utility(#tx { reward = Reward }, Size) ->
	erlang:trunc(Reward / Size).

tx_propagated_size(#tx{ format = 2 }) ->
	?TX_SIZE_BASE;
tx_propagated_size(#tx{ format = 1, data = Data }) ->
	?TX_SIZE_BASE + byte_size(Data).

tx_queue_size(#tx{ format = 1 } = TX) ->
	{tx_propagated_size(TX), 0};
tx_queue_size(#tx{ format = 2, data = Data }) ->
	{?TX_SIZE_BASE, byte_size(Data)}.

record_propagation_rate(PropagatedSize, PropagationTimeUs) ->
	BitsPerSecond = PropagatedSize * 1000000 / PropagationTimeUs * 8,
	prometheus_histogram:observe(tx_propagation_bits_per_second, BitsPerSecond).

record_queue_size(QSize) ->
	prometheus_gauge:set(tx_queue_size, QSize).
