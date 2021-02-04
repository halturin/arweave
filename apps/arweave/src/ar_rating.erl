%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

%%% @doc Rating. We should compute rating for every single peer in
%%%
%%% order to give highest priority for the good nodes and decrease
%%% an influence of the bad ones (including the ban). Here are 3 kind
%%% of variables for the Rating formula - positive, negative, by value.
%%% Positive variables:
%%%   * Join bonus - provides to every new node we have no record before in
%%%                  the rating table
%%%   * Response
%%%   * Mining
%%%     * receive new valid block
%%% Negative variables:
%%%   * Bad/Wrong response on our request
%%%     - malformed
%%%     - 404 for the data
%%%   * Bad/Wrong request
%%%     - to get any information
%%%     - to post (tx, block)
%%% By value:
%%%   * Time response - descrease rating for the slowest peers and increase
%%%                     for the others
%%%
%%%   * Lifespan -  age influencing. its getting bigger by the type from 0 to 1
%%%
%%%                                 1
%%%                influence = ------------ + 1
%%%                             -EXP(T/S)
%%%
%%%				T - number of days since we got this peer
%%%				S - how slow this influence should growth
%%% @end
-module(ar_rating).

-behaviour(gen_server).

-export([
	start_link/0,
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-export([
	get/1,
	set_option/2,
	get_option/1
]).

-include_lib("arweave/include/ar_rating.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-define(COMPUTE_RATING_PERIOD, 60000).

% This record is using as a data structure for the 'rating' database (RocksDB)
-record(rating, {
	% Keep the date of starting this peering.
	since = os:system_time(second),
	% Just a basic level of trust for the newbies (or respawned peering).
	base_bonus = 1000,
	% Bonus and penalties for the responses from the peer
	resp_bonus = 0,
	resp_penalty = 0,
	% ... for the requests for any kind of information (chunks, blocks, etc...)
	req_bonus = 0,
	req_penalty = 0,
	% ... for the sharing txs,blocks with our node
	push_bonus = 0,
	push_penalty = 0,
	% when it was last time updated
	last_update = os:system_time(second)
}).

%% Internal state definition.
-record(state, {
	% do nothing if this node isn't joined to the arweave network
	joined = false,
	% a map of peer => rating
	peers = #{},
	% Set of options how we should compute and keep the rating
	options = #{
		% Period of time we should count requests (in sec) in order
		% to compute value Request per Period
		request_time_period => 60,

		% 'starter pack' for the new comming peers
		base_bonus => 1000
	},
	% recompute ratings for the peers who got updates
	% and write them into the DB
	peers_got_changes = #{},

	rates = #{
		% bonuses
		{request, tx} => 10,
		{request, block} => 20,
		{request, chunk} => 30,
		{push, tx} => 100,
		{push, block} => 200,
		% Rate for the response = 1000 - T (in ms). longer response could make this value negative
		{response, tx} => 1000,
		{response, block} => 2000,
		{response, chunk} => 3000,
		{response, any} => 100,
		% penalties
		{request, malformed} => -10000,
		{response, malformed} => -10000,
		{response, timeout} => -1000,
		{response, not_found} => -500,
		{push, malformed} => -10000,
		attack => -10000000000000000
	},

	% RocksDB reference
	db
}).

%%%===================================================================
%%% API
%%%===================================================================
get(Peer) ->
	gen_server:call(?MODULE, {get_rating, Peer}).

set_option(Option, Value) ->
	gen_server:call(?MODULE, {set, Option, Value}).
get_option(Option) ->
	gen_server:call(?MODULE, {get, Option}).

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
	process_flag(trap_exit, true),
	{ok, Ref} = ar_kv:open("ratings"),
	ar_events:subscribe([network, peer, blocks, txs, chunks, attack]),
	erlang:send_after(?COMPUTE_RATING_PERIOD, ?MODULE, {'$gen_cast', compute_ratings}),
	{ok, #state{db = Ref}}.

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
handle_call({get_rating, _Peer}, _From, State) ->
	{reply, 1, State};
handle_call({set, Option, Value}, _From, State) ->
	Options = maps:put(Option, Value, State#state.options),
	{reply, ok, State#state{options = Options}};
handle_call({get, Option}, _From, State) ->
	Value = maps:get(Option, State#state.options, unknown),
	{reply, Value, State};
handle_call(Request, _From, State) ->
	?LOG_ERROR([{event, unhandled_call}, {message, Request}]),
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
handle_cast(compute_ratings, State) ->
	PeersGotChanges = State#state.peers_got_changes,
	Peers = State#state.peers,
	DB = State#state.db,
	Peers1 = maps:fold(fun (Peer, _Value, Ps) ->
							update_rating(Peer, Ps, DB)
					   end,
					   Peers,
					   PeersGotChanges),

	erlang:send_after(?COMPUTE_RATING_PERIOD, ?MODULE, {'$gen_cast', compute_ratings}),
	State1 = State#state{
		peers_got_changes = #{},
		peers = Peers1
	},
	{noreply, State1};
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
handle_info({event, network, joined}, State) ->
	{noreply, State#state{joined = true}};
handle_info({event, _, _}, State) when State#state.joined == false ->
	% ignore everything until node has joined to the arweave network
	{noreply, State};
handle_info({event, network, left}, State) ->
	{noreply, State#state{joined = false}};
% requests from the peer
handle_info({event, peer, {Act, Kind, Request}}, State)
  	when is_record(Request, event_peer) ->
	Peer = Request#event_peer.peer,
	Rate = maps:get({Act, Kind}, State#state.rates, 0),
	% Decrease Rate on a Time value - longest response whould get lowest rate.
	% In case of negative - count it as a penalty
	Time = Request#event_peer.time,
	case maps:get(Peer, State#state.peers, undefined) of
		undefined ->
			{noreply, State};
		_ when Rate == 0 ->
			% do nothing.
			{noreply, State};
		Rating when Act == push, Rate > 0  ->
			Rating1 = Rating#rating{
				push_bonus = Rating#rating.push_bonus + Rate,
				last_update = os:system_time(second)
			},
			Peers = maps:put(Peer, Rating1, State#state.peers),
			PeersGotChanges = maps:put(Peer, true, State#state.peers_got_changes),
			{noreply, State#state{peers = Peers, peers_got_changes = PeersGotChanges}};
		Rating when Act == push ->
			Rating1 = Rating#rating{
				push_penalty = Rating#rating.push_penalty + Rate,
				last_update = os:system_time(second)
			},
			Peers = maps:put(Peer, Rating1, State#state.peers),
			PeersGotChanges = maps:put(Peer, true, State#state.peers_got_changes),
			{noreply, State#state{peers = Peers, peers_got_changes = PeersGotChanges}};
		Rating when Act == response, Rate-Time > 0  ->
			Rating1 = Rating#rating{
				resp_bonus = Rating#rating.resp_bonus + Rate - Time,
				last_update = os:system_time(second)
			},
			Peers = maps:put(Peer, Rating1, State#state.peers),
			PeersGotChanges = maps:put(Peer, true, State#state.peers_got_changes),
			{noreply, State#state{peers = Peers, peers_got_changes = PeersGotChanges}};
		Rating when Act == response ->
			Rating1 = Rating#rating{
				resp_penalty = Rating#rating.resp_penalty + Time - Rate,
				last_update = os:system_time(second)
			},
			Peers = maps:put(Peer, Rating1, State#state.peers),
			PeersGotChanges = maps:put(Peer, true, State#state.peers_got_changes),
			{noreply, State#state{peers = Peers, peers_got_changes = PeersGotChanges}};
		Rating when Act == request, Rate > 0  ->
			Rating1 = Rating#rating{
				req_bonus = Rating#rating.req_bonus + Rate,
				last_update = os:system_time(second)
			},
			Peers = maps:put(Peer, Rating1, State#state.peers),
			PeersGotChanges = maps:put(Peer, true, State#state.peers_got_changes),
			{noreply, State#state{peers = Peers, peers_got_changes = PeersGotChanges}};
		Rating when Act == request ->
			Rating1 = Rating#rating{
				req_penalty = Rating#rating.req_penalty + Rate,
				last_update = os:system_time(second)
			},
			Peers = maps:put(Peer, Rating1, State#state.peers),
			PeersGotChanges = maps:put(Peer, true, State#state.peers_got_changes),
			{noreply, State#state{peers = Peers, peers_got_changes = PeersGotChanges}}

	end;

% just got a new peer
handle_info({event, peer, {joined, Peer}}, State) ->
	% check whether we had a peering with this Peer
	Bin = term_to_binary(Peer),
	Rating = case ar_kv:get(State#state.db, Bin) of
		not_found ->
			R = #rating{},
			ok = ar_kv:put(State#state.db, Bin, term_to_binary(R)),
			R;
		{ok, R} ->
			binary_to_term(R)
	end,
	Peers = maps:put(Peer, Rating, State#state.peers),
	{noreply, State#state{peers = Peers}};

% peer just left
handle_info({event, peer, {left, Peer}}, State) ->
	Peers = maps:remove(Peer, State#state.peers),
	{noreply, State#state{peers = Peers}};

handle_info({event, attack, Peer}, State) ->
	Rate = maps:get(attack, State#state.rates, -10000000000000000),
	Rating = #rating{base_bonus = Rate},
	Peers = maps:put(Peer, Rating, State#state.peers),
	update_rating(Peer, Peers, State#state.db),
	{noreply, State#state{peers = Peers}};

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
terminate(_Reason, State) ->
	ar_kv:close(State#state.db),
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
update_rating(Peer, Peers, DB) ->
	case maps:get(Peer, Peers, unknown) of
		unknown ->
			Peers;
		Rating ->
			ar_kv:put(DB, Peer, Rating),
			R = Rating#rating.base_bonus +
				Rating#rating.resp_bonus +
				Rating#rating.resp_penalty +
				Rating#rating.req_bonus +
				Rating#rating.req_penalty +
				Rating#rating.push_bonus +
				Rating#rating.push_penalty,
			ban_if_deserved(Peer, R)
	end.

ban_if_deserved(Peer, Rating) when Rating < 0 ->
	Until = os:system_time(second) + Rating * 60,
	ar_events:send(access, {ban, Peer, Until});
ban_if_deserved(_Peer, _Rating) ->
	ok.
