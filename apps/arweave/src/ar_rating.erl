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
%%%   * Lifespan - too short lifespan, as well as too long one, should decrease
%%%                peer's rating in order to keep motivation of rotating/refreshing
%%%                in the 'favorite list'
%%%
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

% This record is using as a data structure for the 'rating' database (RocksDB)
-record(rating, {
	% Keep the date of starting this peering.
	since = os:system_time(),

	% Just a basic level of trust for the newbies (or respawned peering).
	base_bonus = 100,

	% Bonus and penalties for the responses from the peer
	resp_bonus,
	resp_penalty,

	% ... for the requests for any kind of information (chunks, blocks, etc...)
	req_bonus,
	req_penalty,

	% ... for the sharing txs,blocks with our node
	push_bonus,
	push_penalty,

	% We should keep the information about any peer we had a relationship with.
	% Once a peering has ended up, this record is stored into the 'respawn' list as a value
	% {Until, Rating} and the rest fields are zeroing out.
	% Keep last N items of peer relationship, just for history. Might be used
	% in the future (some global reputation)
	respawns = []
}).

%% Internal state definition.
-record(state, {
	% do nothing if this node isn't joined to the arweave network
	joined = false,
	% a map of peer => rating
	peers = #{},
	% Set of options how we should compute and keep the rating
	options = #{
		% Number of items we should use to compute moving average
		% of the response time.
		response_time_MA_n => 10,

		% Period of time we should count requests (in sec) in order
		% to compute value Request per Period
		request_time_period => 60,

		% A number of days being a peer. The main idea behind this number
		% is to force this node to refresh peers its working with.
		% The first half of its age this value makes a positive effect
		% (getting better by the time), but the second half it makes
		% a negative effect (increasing negative influence by the time)
		% until the moment this limit has reached.
		peering_age_limit => 14,

		% keep last N records about the peer
		respawn_history_length => 10,

		base_bonus => 100
	},
	responses = #{},

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
	ar_events:subscribe(network),
	ar_events:subscribe(peer),
	ar_events:subscribe(blocks),
	ar_events:subscribe(txs),
	ar_events:subscribe(chunks),
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
handle_info({event, network, join}, State) ->
	{noreply, State#state{joined = true}};
handle_info({event, _, _}, State) when State#state.joined == false ->
	% ignore everything until node has joined to the arweave network
	{noreply, State};
handle_info({event, network, leave}, State) ->
	{noreply, State#state{joined = false}};
% requests from the peer to get an infomation. it can be useful
% to set a limit for req/s for the peers with low rating
handle_info({event, peer, {request, malformed, Request}}, State)
  	when is_record(Request, event_peer_request) ->
	{noreply, State};
handle_info({event, peer, {request, txs, Request}}, State)
  	when is_record(Request, event_peer_request) ->
	{noreply, State};
handle_info({event, peer, {request, _Kind, Request}}, State)
  	when is_record(Request, event_peer_request) ->
	% do nothing for the rest
	{noreply, State};
% request from the peer to put an information
handle_info({event, peer, {push, malformed, Request}}, State)
  	when is_record(Request, event_peer_request) ->
	{noreply, State};
handle_info({event, peer, {push, txs, Request}}, State)
  	when is_record(Request, event_peer_request) ->
	{noreply, State};
handle_info({event, peer, {push, blocks, Request}}, State)
  	when is_record(Request, event_peer_request) ->
	{noreply, State};
% response from the peer on our request
handle_info({event, peer, {response, malformed, Response}}, State)
  	when is_record(Response, event_peer_response) ->
	{noreply, State};
handle_info({event, peer, {response, timeout, Response}}, State)
  	when is_record(Response, event_peer_response) ->
	{noreply, State};
handle_info({event, peer, {response, not_found, Response}}, State)
  	when is_record(Response, event_peer_response) ->
	{noreply, State};
handle_info({event, peer, {response, _Any, Response}}, State)
  	when is_record(Response, event_peer_response) ->
	{noreply, State};
% just got a new peer
handle_info({event, peer, {join, Peer}}, State) ->
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
