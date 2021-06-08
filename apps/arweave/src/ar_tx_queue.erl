-module(ar_tx_queue).

-behaviour(gen_server).

%% API
-export([
	start_link/0,
	set_max_emitters/1,
	set_max_header_size/1,
	set_max_data_size/1,
	set_pause/1,
	set_num_peers_tx_broadcast/1,
	show_queue/0,
	utility/1,
	drop_tx/1
]).

%% gen_server callbacks
-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {
	tx_queue,
	emitters_running,
	max_emitters,
	max_header_size,
	max_data_size,
	header_size,
	data_size,
	paused,
	emit_map,
	num_peers_tx_broadcast
}).

-define(EMITTER_START_WAIT, 150).
-define(EMITTER_INTER_WAIT, 5).

%% Prioritize format=1 transactions with data size bigger than this
%% value (in bytes) lower than every other transaction. The motivation
%% is to encourage people uploading data to use the new v2 transaction
%% format. Large v1 transactions may significantly slow down the rate
%% of acceptance of transactions into the weave.
-define(DEPRIORITIZE_V1_TX_SIZE_THRESHOLD, 100).

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
%%% API
%%%===================================================================

set_pause(PauseOrNot) ->
	gen_server:call(?MODULE, {set_pause, PauseOrNot}).

set_max_header_size(Bytes) ->
	gen_server:call(?MODULE, {set_max_header_size, Bytes}).

set_max_data_size(Bytes) ->
	gen_server:call(?MODULE, {set_max_data_size, Bytes}).

set_max_emitters(N) ->
	gen_server:call(?MODULE, {set_max_emitters, N}).

set_num_peers_tx_broadcast(N) when N > 0->
	gen_server:call(?MODULE, {set_num_peers_tx_broadcast, N}).

show_queue() ->
	gen_server:call(?MODULE, show_queue).

drop_tx(TX) ->
	gen_server:cast(?MODULE, {drop_tx, TX}).

%%%===================================================================
%%% Generic server callbacks.
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
	MaxEmitters =
		case ar_meta_db:get(max_emitters) of
			not_found -> ?NUM_EMITTER_PROCESSES;
			X -> X
		end,
	gen_server:cast(?MODULE, start_emitters),
	{ok, #state{
		tx_queue = gb_sets:new(),
		emitters_running = 0,
		max_emitters = MaxEmitters,
		max_header_size = ?TX_QUEUE_HEADER_SIZE_LIMIT,
		max_data_size = ?TX_QUEUE_DATA_SIZE_LIMIT,
		header_size = 0,
		data_size = 0,
		paused = false,
		emit_map = #{},
		num_peers_tx_broadcast = ?NUM_PEERS_TX_BROADCAST
	}}.

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
handle_call({set_pause, PauseOrNot}, _From, State) ->
	{reply, ok, State#state{ paused = PauseOrNot }};

handle_call({set_max_emitters, N}, _From, State) ->
	?LOG_DEBUG("TX Queue set max emmiters to: ~p", [N]),
	{reply, ok, State#state{ max_emitters = N }};

handle_call({set_max_header_size, Bytes}, _From, State) ->
	?LOG_DEBUG("TX Queue set max header size to: ~p", [Bytes]),
	{reply, ok, State#state{ max_header_size = Bytes }};

handle_call({set_max_data_size, Bytes}, _From, State) ->
	?LOG_DEBUG("TX Queue set max data size to: ~p", [Bytes]),
	{reply, ok, State#state{ max_data_size = Bytes }};

handle_call({set_num_peers_tx_broadcast, N}, _From, State) ->
	?LOG_DEBUG("TX Queue set max number peers for tx broadcast to: ~p", [N]),
	{reply, ok, State#state{ num_peers_tx_broadcast = N}};

handle_call(show_queue, _From, State = #state{ tx_queue = Q }) ->
	Reply = show_queue(Q),
	{reply, Reply, State};

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
handle_cast(start_emitters, State) ->
	#state{ max_emitters = MaxEmitters, emitters_running = EmittersRunning } = State,
	lists:foreach(
		fun(N) ->
			Wait = N * ?EMITTER_START_WAIT,
			erlang:send_after(Wait, ?MODULE, {'$gen_cast', emitter_go})
		end,
		lists:seq(1, max(MaxEmitters - EmittersRunning, 0))
	),
	{noreply, State#state{ emitters_running = MaxEmitters, paused = false }};

handle_cast(emitter_go, State = #state{ paused = true }) ->
	?LOG_DEBUG("TX Queue processing is paused. Waiting..."),
	erlang:send_after(?EMITTER_START_WAIT, ?MODULE, {'$gen_cast', emitter_go}),
	{noreply, State};

handle_cast(emitter_go, State = #state{ emitters_running = EmittersRunning, max_emitters = MaxEmitters }) when EmittersRunning > MaxEmitters ->
	{noreply, State#state { emitters_running = EmittersRunning - 1}};

handle_cast(emitter_go, State) ->
	#state{
		tx_queue = Q,
		header_size = HeaderSize,
		data_size = DataSize
	} = State,
	NewState =
		case gb_sets:is_empty(Q) of
			true ->
				erlang:send_after(?EMITTER_START_WAIT, ?MODULE, {'$gen_cast', emitter_go}),
				State;
			false ->
				{{_, {TX, {TXHeaderSize, TXDataSize}}}, NewQ} = gb_sets:take_largest(Q),
				gen_server:cast(?MODULE, {emit_tx, TX}),
				State#state{
					tx_queue = NewQ,
					header_size = HeaderSize - TXHeaderSize,
					data_size = DataSize - TXDataSize
				}
		end,
	{noreply, NewState};

handle_cast({emitted_tx, {Reply, TX}}, State = #state{ emit_map = EmitMap, tx_queue = Q }) ->
	TXID = TX#tx.id,
	case EmitMap of
		#{ TXID := #{ started_at := StartedAt } } ->
			PropagationTimeUs = timer:now_diff(erlang:timestamp(), StartedAt),
			record_propagation_status(Reply),
			record_propagation_rate(tx_propagated_size(TX), PropagationTimeUs),
			record_queue_size(gb_sets:size(Q)),
			gen_server:cast(?MODULE, {emitter_finished, TX}),
			{noreply, State#state{ emit_map = maps:remove(TXID, EmitMap) }};
		_ ->
			?LOG_ERROR("Emitted TX is missing in emit_map. Please, report this issue"),
			gen_server:cast(?MODULE, {emit_tx, TX}),
			{noreply, State}
	end;

handle_cast({emit_tx, TX}, State) ->
	EmitMap = State#state.emit_map,
	ExcludePeers = #{},
	NumPeersBroadcast = State#state.num_peers_tx_broadcast,
	Emitter = spawn(
		fun() ->
			Reply = ar_network:broadcast(tx, TX, {ExcludePeers, NumPeersBroadcast}),
			gen_server:cast(?MODULE, {emitted_tx, {Reply, TX}})
		end
	),
	TXID = TX#tx.id,
	NewEmitMap = EmitMap#{
		TXID => #{
			started_at => erlang:timestamp(),
			emitter => Emitter % keep the PID of spawned emitter process
		}},
	{noreply, State#state{ emit_map = NewEmitMap}};

handle_cast({emitter_finished, TX}, State) ->
	% Sending to the mining pool should be delayed according to its size.
	Delay = ar_node_utils:calculate_delay(tx_propagated_size(TX)),
	erlang:send_after(Delay, ?MODULE, {'$gen_cast', {send_to_mining_pool, TX}}),
	erlang:send_after(?EMITTER_INTER_WAIT, ?MODULE, {'$gen_cast', emitter_go}),
	{noreply, State};

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
	?LOG_DEBUG("TX Queue new tx received ~p from ~p", [ar_util:encode(TX#tx.id), FromPeerID]),
	{TXHeaderSize, TXDataSize} = tx_queue_size(TX),
	U = utility(TX),
	Item = {U, {TX, {TXHeaderSize, TXDataSize}}},
	?LOG_ERROR("SSSS HS ~p DS ~p TXHS ~p TXDS ~p", [HeaderSize, DataSize, TXHeaderSize, TXDataSize]),
	case gb_sets:is_element(Item, Q) of
		true ->
			{noreply, State};
		false ->
			{NewQ, {NewHeaderSize, NewDataSize}, DroppedTXs} =
				maybe_drop(
					gb_sets:add_element(Item, Q),
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
							ar_events:send(tx, {drop, DroppedTX, removed_from_tx_queue}),
							ar_util:encode(DroppedTX#tx.id)
						end,
						DroppedTXs
					),
					?LOG_INFO([
						{event, drop_txs_from_queue},
						{dropped_txs, DroppedIDs}
					])
			end,
			NewState = State#state{
				tx_queue = NewQ,
				header_size = NewHeaderSize,
				data_size = NewDataSize
			},
			{noreply, NewState}
	end;

handle_info({event, tx, {drop, TX, Reason}}, State) ->
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
			?LOG_DEBUG("Drop TX from queue ~p with reason: ~p", [ar_util:encode(TX#tx.id), Reason]),
			{noreply, State#state{
				tx_queue = gb_sets:del_element(Item, Q),
				header_size = HeaderSize - TXHeaderSize,
				data_size = DataSize - TXDataSize
			}};
		false ->
			{noreply, State}
	end;

handle_info({event, tx, _Event}, State) ->
	% ignore the rest of tx events
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
%%% Private functions.
%%%===================================================================

maybe_drop(Q, Size, MaxSize) ->
	maybe_drop(Q, Size, MaxSize, []).

maybe_drop(Q, {HeaderSize, DataSize} = Size, {MaxHeaderSize, MaxDataSize} = MaxSize, DroppedTXs) ->
	case HeaderSize > MaxHeaderSize orelse DataSize > MaxDataSize of
		true ->
			{{_, {TX, {DroppedHeaderSize, DroppedDataSize}}}, NewQ} = gb_sets:take_smallest(Q),
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
		fun({_, {TX, _}}, Acc) ->
			[{ar_util:encode(TX#tx.id), TX#tx.reward, TX#tx.data_size} | Acc]
		end,
		[],
		Q
	).

utility(TX = #tx{ data_size = DataSize }) ->
	utility(TX, ?TX_SIZE_BASE + DataSize).

utility(#tx{ format = 1, reward = Reward, data_size = DataSize }, Size)
		when DataSize > ?DEPRIORITIZE_V1_TX_SIZE_THRESHOLD ->
	{1, erlang:trunc(Reward / Size)};
utility(#tx{ reward = Reward }, Size) ->
	{2, erlang:trunc(Reward / Size)}.

tx_propagated_size(#tx{ format = 2 }) ->
	?TX_SIZE_BASE;
tx_propagated_size(#tx{ format = 1, data = Data }) ->
	?TX_SIZE_BASE + byte_size(Data).

tx_queue_size(#tx{ format = 1 } = TX) ->
	{tx_propagated_size(TX), 0};
tx_queue_size(#tx{ format = 2, data = Data }) ->
	{?TX_SIZE_BASE, byte_size(Data)}.

record_propagation_status([]) ->
	ok;
record_propagation_status([{timeout, _} | Results ]) ->
	prometheus_counter:inc(propagated_transactions_total, [timeout]),
	record_propagation_status(Results);
record_propagation_status([{nopeers, _, _} | Results ]) ->
	prometheus_counter:inc(propagated_transactions_total, [nopeers]),
	record_propagation_status(Results);
record_propagation_status([error | Results ]) ->
	prometheus_counter:inc(propagated_transactions_total, [error]),
	record_propagation_status(Results);
record_propagation_status([Result | Results ]) ->
	prometheus_counter:inc(propagated_transactions_total, [Result]),
	record_propagation_status(Results).

record_propagation_rate(PropagatedSize, PropagationTimeUs) ->
	BitsPerSecond = PropagatedSize * 1000000 / PropagationTimeUs * 8,
	prometheus_histogram:observe(tx_propagation_bits_per_second, BitsPerSecond).

record_queue_size(QSize) ->
	prometheus_gauge:set(tx_queue_size, QSize).
