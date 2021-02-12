%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

%%% @doc Rating. We should compute rating for every single peer in
%%% order to give highest priority for the good nodes and decrease
%%% an influence of the bad ones (including ban for the bad behavior).
%%% Here are 3 kind of variables for the Rating formula - positive, negative, by value.
%%% Positive variables:
%%%   * Join bonus (aka base bonus) - provides for the new node we had
%%%     no record before in the rating table
%%%   * Response - getting response for the request with depending of the time response
%%%   * Push - peer shares an information
%%% Negative variables:
%%%   * Bad/Wrong response on our request
%%%     - malformed
%%%     - 404 for the data
%%%     - timeouts
%%%   * Bad/Wrong request
%%%     - to get any information
%%%     - to post (tx, block)
%%% By value:
%%%   * Time response - descrease rating for the slowest peers and increase
%%%                     for the others
%%%   * Lifespan -  age influencing. its getting bigger by the time from 0 to 1
%%%
%%%                                 1
%%%                influence = ------------ + 1
%%%                             -EXP(T/S)
%%%
%%%				T - number of days since we got this peer
%%%				S - how slow this influence should growth
%%%
%%%	This module also provides triggering mechanic in order to handle conditioned
%%%	behaviour (if some action was repeated N times during the period P).
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
	get_option/1,
	get_banned/0
]).


-include_lib("arweave/include/ar_rating.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-define(COMPUTE_RATING_PERIOD, 60000).

% This record is using as a data structure for the 'rating' database (RocksDB)
-record(rating, {
	% rating value
	r = 0,
	% Keep the date of starting this peering.
	since = os:system_time(second),
	% rate group keeps the accumulated value of rated action which
	% is defined in 'rates' map.
	% Key:
	%   is the tuple with two values
	%   {Act, Positive}
	% 	Act - is the first value of a key tuple in the rates map
	% 	Positive - true or false.
	% Value:
	%   tuple with two values
	%   {N, History}
	%   N - accumulated value
	%   History - list of timestamps
	% example: {response, false} => {-123, [1613147757, 1613147333]}
	rate_group = #{},
	% when it was last time updated
	last_update = os:system_time(second)
}).

-define(BASE_SHIFT, 32).
-define(CLEAR(X), ((1 bsl ?BASE_SHIFT - 1) band X)).
% variative values
-define(MINUS_TIME, (1 bsl ?BASE_SHIFT)).
-define(PLUS_TIME, (1 bsl (?BASE_SHIFT+1))).
% if you want to add yet another variative value
% you should define it here like:
% 	-define(YET_ANOTHER1, (1 bsl (?BASE_SHIFT+2))).
% 	-define(YET_ANOTHER2, (1 bsl (?BASE_SHIFT+3))).

-define(RATE(X, T), begin
	?CLEAR(X)
 	- T*(X band ?MINUS_TIME bsr ?BASE_SHIFT)
 	+ T*(X band ?PLUS_TIME bsr (?BASE_SHIFT+1))
	% Don't forget to add YET_ANOTHER1 and YET_ANOTHER2 here
	% using the same way like
	%	+ V/(X band ?YET_ANOTHER1 bsr (?BASE_SHIFT + 2))
	%	- V*(X band ?YET_ANOTHER2 bsr (?BASE_SHIFT + 3))
	% And for sure, V should be added as an argument
	%	-define(RATE(X, T, V), begin...
	end).

%% Internal state definition.
-record(state, {
	% Are we connected to the arweave network?
	joined = false,

	% Recompute ratings for the peers who got updates
	% and write them into the RocksDB
	peers_got_changes = #{},

	% Rating map defines rate for the action.
	% Key must be a tuple with two values
	% 	{Action, ActionType}
	% Value just a number. Use macro definition along with 'bor' operator
	% to enable variative value.
	% 	MINUS_TIME - result will be decreased on a number of ms
	% 	PLUS_TIME - result will be increased on a number of ms
	rates = #{
		% bonuses
		{request, tx} => 10,
		{request, block} => 20,
		{request, chunk} => 30,
		% Rate for the push/response = Bonus - T (in ms). longer time could make this value negative
		{push, tx} => 1000 bor ?MINUS_TIME,
		{push, block} => 2000 bor ?MINUS_TIME,
		{response, tx} => 1000 bor ?MINUS_TIME,
		{response, block} => 2000 bor ?MINUS_TIME,
		{response, chunk} => 3000 bor ?MINUS_TIME,
		{response, any} => 1000 bor ?MINUS_TIME,
		% penalties
		{request, malformed} => -1000,
		{response, malformed} => -10000,
		{response, request_timeout} => -1000,
		{response, connect_timeout} => 0,
		{response, not_found} => -500,
		{push, malformed} => -10000,
		{attack, any} => -10000
		% after defining a new variative value you can use it here
		% {example, a} => 100 bor ?YET_ANOTHER1
		% {example, b} => 200 bor ?YET_ANOTHER2
		% or along with the other variatives
		% {example, c} => 200 bor ?YET_ANOTHER2 bor ?MINUS_TIME
	},

	% Call Trigger(Value) if event happend N times during period P(in sec).
	% {act, kind} => {N, P, Trigger, Value}.
	% Triggering call happens if it has a rate with the same name {act, kind}.
	% Otherwise it will be ignored.
	triggers = #{
		% If we got 30 blocks during last hour from the same peer
		% lets provide an extra bonus for the stable peering.
		{push, block} => {30, 3600, bonus, 500},
		% ban for an hour for the malformed request (10 times during an hour)
		{request, malformed} => {10, 3600, ban, 60},
		% Exceeding the limit of 60 requests per 1 minute
		% decreases rate by 10 points.
		{request, tx} => {60, 60, penalty, 10},
		% If we got timeout few times we should handle it as a peer
		% disconnection with removing it from the rating. We also
		% have to inform the other processes that its went offline.
		% Once the last peer went offline this node should handle
		% the disconnection process from the arweave network.
		{response, connect_timeout} => {5, 300, offline, 0},
		% Instant ban for the attack (for the next 24 hours).
		{attack, any} => {1, 0, ban, 1440}
	},

	% RocksDB reference
	db
}).

%%%===================================================================
%%% API
%%%===================================================================
get(Peer) ->
	gen_server:call(?MODULE, {get_rating, Peer}).
get_banned() ->
	%
	[].

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
	{ok, Ref} = ar_kv:open("ratings"),
	ar_events:subscribe([network, peer, blocks, txs, chunks, attack]),
	erlang:send_after(?COMPUTE_RATING_PERIOD, ?MODULE, {'$gen_cast', compute_ratings}),
	% having at least 1 record means this process has been restarted (due to process fail)
	% and we already joined to the arweave network
	case ets:info(?MODULE, size) of
		0 ->
			{ok, #state{db = Ref}};
		_ ->
			{ok, #state{db = Ref, joined = true}}
	end.

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
	DB = State#state.db,
	maps:map(
		fun(Peer, _Value) ->
			update_rating(Peer, DB)
		end,
		PeersGotChanges
	),
	erlang:send_after(?COMPUTE_RATING_PERIOD, ?MODULE, {'$gen_cast', compute_ratings}),
	{noreply, State#state{peers_got_changes = #{}} };
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
% uncomment these lines once we implement join/leave arweave network event
%handle_info({event, _, _}, State) when State#state.joined == false ->
%	% ignore everything until node has joined to the arweave network
%	{noreply, State};
handle_info({event, network, left}, State) ->
	{noreply, State#state{joined = false}};

handle_info({event, peer, {Act, Kind, Request}}, State)
	when is_record(Request, event_peer) ->
	Peer = Request#event_peer.peer,
	Time = Request#event_peer.time,
	Rate = ?RATE(maps:get({Act, Kind}, State#state.rates, 0),
				 Time
				 % if a new variative value was defined it should be
				 % added here as an argument
				),
	Trigger = maps:get({Act, Kind}, State#state.triggers, undefined),
	T = os:system_time(second),
	case ets:lookup(?MODULE, {peer, Peer}) of
		[] ->
			{noreply, State};
		_ when Rate == 0 ->
			% do nothing.
			{noreply, State};
		[{_, Rating}] ->
			Positive = Rate > 0,
			{R, History} = maps:get({Act, Positive}, Rating#rating.rate_group, {0, []}),
			{ExtraRate, History1} = trigger(Trigger, Peer, History, T),
			R1 = R + Rate + ExtraRate,
			RG = maps:put({Act, Positive}, {R1, History1}, Rating#rating.rate_group),
			Rating1 = Rating#rating{
				rate_group = RG,
				last_update = T
			},
			ets:insert(?MODULE, {{peer, Peer}, Rating1}),
			PeersGotChanges = maps:put(Peer, true, State#state.peers_got_changes),
			{noreply, State#state{peers_got_changes = PeersGotChanges}}
	end;

% just got a new peer
handle_info({event, peer, {joined, Peer}}, State) ->
	% check whether we had a peering with this Peer
	BinPeer = term_to_binary(Peer),
	Rating= case ar_kv:get(State#state.db, BinPeer) of
		not_found ->
			R = #rating{},
			ok = ar_kv:put(State#state.db, BinPeer, term_to_binary(R)),
			R;
		{ok, R} ->
			binary_to_term(R)
	end,
	ets:insert(?MODULE, {{peer, Peer}, Rating}),
	{noreply, State};

% peer just left
handle_info({event, peer, {left, Peer}}, State) ->
	update_rating(Peer, State#state.db),
	ets:delete(?MODULE, {peer, Peer}),
	case ets:info(?MODULE, size) of
		0 ->
			% it was the last one. now we are disconnected from
			% the arweave network and should initiate the joining
			% process again
			ar_events:send(network, left),
			{noreply, State};
		_ ->
			{noreply, State}
	end;

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
update_rating(Peer, DB) ->
	case ets:lookup(?MODULE, {peer, Peer}) of
		[] ->
			ok;
		[{_, Rating}] ->
			% Compute age in days
			Age = (os:system_time(second) - Rating#rating.since)/(60*60*24),
			% The influence is getting close to 1 during the time. Division by 3 makes
			% this value much close to 1 in around 10 days. Increasing divider makes
			% this transition longer.
			Influence = (1/-math:exp(Age/3))+1,
			% Sum up all the numbers.
			R = lists:sum(maps:fold(fun(_,{N,_},A) -> [N|A] end, [], Rating#rating.rate_group)),
			% Apply Influence and store the result.
			Rating1 = Rating#rating{r = trunc(R * Influence)},
			BinPeer = term_to_binary(Peer),
			BinRating = term_to_binary(Rating1),
			ar_kv:put(DB, BinPeer, BinRating),
			ets:insert(?MODULE, {{peer, Peer}, Rating1})
	end.

trigger(undefined, _Peer, History, _T) ->
	{0, History};

trigger({_N, _P, _, _V}, Peer, [H|_] = History, T) when H > T ->
	% The last timestamp was added to the History is in the future (more
	% than time T) it means we got event from banned peer.
	% Send 'ban' again until the time H
	ar_events:send(access, {ban, Peer, H}),
	{0, History};
trigger({_N, P, _, _V}, _Peer, [H|_], T) when T - H > P ->
	% Last event happened longer than P seconds ago, so we dont
	% need to keep old values. Keep the current one only.
	{0, [T]};
trigger({N, _P, _, _V}, _Peer, History, T) when length(History)+1 < N ->
	% not enough events for the triggering. just keep it.
	{0, [T|History]};
trigger({N, P, Trigger, V}, Peer, History, T) ->
	History1 = [T|History],
	Period = T - lists:nth(N, History1),
	case Period > P of
		true when length(History1) > N ->
			{V, lists:sublist(History1, N)};
		true ->
			{V, History1};
		_ when Trigger == ban ->
			% for 'ban' the value of V is in minutes
			BanPeriod = V*60,
			BanTime = T + BanPeriod,
			ar_events:send(access, {ban, Peer, BanTime}),
			{0, [BanTime | History1]};
		_ when Trigger == bonus ->
			{V, History1};
		_ when Trigger == penalty ->
			{-V, History1};
		_ when Trigger == offline ->
			ar_events:send(peer, {left, Peer}),
			{0, History1}
	end.

%%
%% Unit-tests
%%

-include_lib("eunit/include/eunit.hrl").

trigger_undefined_test() ->
	?assertMatch(
		{0, [1]},
		trigger(undefined, peer1, [1], 1)
	).
trigger_banned_test() ->
	[
	 ?assertMatch(
		{0, [5]},
		trigger({1, 1, test, 8}, 0, [5], 4)
	 ),
	 ?assertMatch(
		{0, [5,4,3,2,1]},
		trigger({1, 1, test, 8}, 0, [5,4,3,2,1], 4)
	 )
	].
trigger_empty_test() ->
	?assertMatch(
		{0, [3]},
		trigger({2, 1, test, 8}, peer1, [], 3)
	).
trigger_long_ago_test() ->
	% when the last event happened longer time ago than given period.
	% period = 4
	% current T = 12
	?assertMatch(
		{0, [12]},
		trigger({2, 4, test, 0}, peer1, [5,4,3,2,1], 12)
	).
trigger_cut_the_tail_events_test() ->
	% cut the tail of the event list if it didn't exceed the limit
	% of events for the given period
	?assertMatch(
		{0, [26,25,20]},
		trigger({3, 4, test, 0}, peer1, [25,20,15,10,5,1], 26)
	).

rate_clear_flags_test() ->
	R0 = 100,
	R1 = 200 bor ?PLUS_TIME,
	R2 = 300 bor ?MINUS_TIME,
	R3 = 400 bor ?PLUS_TIME bor ?MINUS_TIME,
	100 = ?CLEAR(R0),
	200 = ?CLEAR(R1),
	300 = ?CLEAR(R2),
	400 = ?CLEAR(R3).

rate_with_enabled_variative_time_test() ->
	R0 = 100,
	R1 = 200 bor ?PLUS_TIME,
	R2 = 300 bor ?MINUS_TIME,
	R3 = 400 bor ?PLUS_TIME bor ?MINUS_TIME,
	% rate has no enabled time influence
	?assertMatch(
		100, % ?CLEAR(R0),
		?RATE(R0, 100)
	),
	% should be increased by 100
	?assertMatch(
		300, % ?CLEAR(R1) + 100,
		?RATE(R1, 100)
	),
	% should be decreased by 500
	?assertMatch(
		-200, % ?CLEAR(R2) - 500,
		?RATE(R2, 500)
	),
	% shouldn't be affected
	?assertMatch(
		400, % ?CLEAR(R3),
		?RATE(R3, 100)
	).

