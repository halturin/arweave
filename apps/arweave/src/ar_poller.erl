%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_poller).
-behaviour(gen_server).

%%% This module fetches blocks from the network in case the node hasn't
%%% received blocks for some other reason.

-export([start_link/0]).

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
	joined = false,
	connected = false,
	last_seen_height = -1,
	interval,
	poll_timer
}).

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
	?LOG_INFO([{event, ar_poller_start}]),
	{ok, Config} = application:get_env(arweave, config),
	ar_events:subscribe(node, network),
	gen_server:cast(?MODULE, poll),
	{ok, #state{
		joined = ar_node:is_joined(),
		connected = ar_network:is_connected(),
		last_seen_height = -1,
		interval = Config#config.polling
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
handle_call(Request, _From, State) ->
	?LOG_ERROR([{event, unhandled_call}, {request, Request}]),
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
handle_cast(poll, State) when not State#state.connected ->
	timer:cancel(State#state.poll_timer),
	{noreply, State};
handle_cast(poll, State) when not State#state.joined->
	timer:cancel(State#state.poll_timer),
	{noreply, State};

handle_cast(poll, State) ->
	?LOG_DEBUG("Polling. Checking current height ..."),
	case ar_node:get_height() of
		Height when Height > State#state.last_seen_height ->
			{ok, T} = timer:send_after(State#state.interval, {'$gen_cast', poll}),
			{noreply, State#state{poll_timer = T, last_seen_height = Height}};
		Height ->
			?LOG_DEBUG("Polling block ~p ...", [Height + 1]),
			case fetch_block(Height + 1) of
				ok ->
					{ok, T} = timer:send_after(State#state.interval, {'$gen_cast', poll}),
					{noreply, State#state{ poll_timer = T, last_seen_height = Height + 1 }};
				{error, _} ->
					{ok, T} = timer:send_after(State#state.interval, {'$gen_cast', poll}),
					{noreply, State#state{ poll_timer = T, last_seen_height = Height }}
			end
	end;

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
handle_info({event, node, ready}, State) ->
	?LOG_DEBUG("Start polling..."),
	gen_server:cast(?MODULE, poll),
	{noreply, State#state{joined = true}};
handle_info({event, node, _}, State) ->
	timer:cancel(State#state.poll_timer),
	{noreply, State};
handle_info({event, network, connected}, State) when State#state.joined ->
	?LOG_DEBUG("Start polling..."),
	gen_server:cast(?MODULE, poll),
	{noreply, State#state{connected = true}};
handle_info({event, network, disconnected}, State) ->
	?LOG_DEBUG("Stop polling. Waiting for the network..."),
	timer:cancel(State#state.poll_timer),
	{noreply, State#state{connected = false}}.

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
	?LOG_INFO([{event, ar_network_terminated}, {reason, Reason}]),
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
%%% Internal functions.
%%%===================================================================

fetch_block(Height) ->
	case ar_network:get_block_shadow(Height) of
		unavailable ->
			{error, block_not_found};
		BShadow ->
			Timestamp = erlang:timestamp(),
			case fetch_previous_blocks(BShadow, Timestamp) of
				ok ->
					ok;
				Error ->
					Error
			end
	end.

fetch_previous_blocks(BShadow, ReceiveTimestamp) ->
	HL = [BH || {BH, _} <- ar_node:get_block_txs_pairs()],
	case fetch_previous_blocks2(BShadow, HL) of
		{ok, FetchedBlocks} ->
			submit_fetched_blocks(FetchedBlocks, ReceiveTimestamp),
			submit_fetched_blocks([BShadow], ReceiveTimestamp);
		{error, _} = Error ->
			Error
	end.

fetch_previous_blocks2(FetchedBShadow, BehindCurrentHL) ->
	fetch_previous_blocks2(FetchedBShadow, BehindCurrentHL, []).

fetch_previous_blocks2(_FetchedBShadow, _BehindCurrentHL, FetchedBlocks)
		when length(FetchedBlocks) >= ?STORE_BLOCKS_BEHIND_CURRENT ->
	{error, failed_to_reconstruct_block_hash_list};

fetch_previous_blocks2(FetchedBShadow, BehindCurrentHL, FetchedBlocks) ->
	PrevH = FetchedBShadow#block.previous_block,
	case lists:dropwhile(fun(H) -> H /= PrevH end, BehindCurrentHL) of
		[PrevH | _] ->
			{ok, FetchedBlocks};
		_ ->
			case ar_network:get_block_shadow(PrevH) of
				unavailable ->
					{error, previous_block_not_found};
				PrevBShadow ->
					fetch_previous_blocks2(
						PrevBShadow,
						BehindCurrentHL,
						[PrevBShadow | FetchedBlocks]
					)
			end
	end.

submit_fetched_blocks([B | Blocks], ReceiveTimestamp) ->
	?LOG_INFO([
		{event, ar_poller_fetched_block},
		{block, ar_util:encode(B#block.indep_hash)},
		{height, B#block.height}
	]),
	ar_events:send(block, {new, B, poller}),
	submit_fetched_blocks(Blocks, ReceiveTimestamp);

submit_fetched_blocks([], _ReceiveTimestamp) ->
	ok.
