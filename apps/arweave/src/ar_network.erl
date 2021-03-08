%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_network).

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
	broadcast/2,
	broadcast/3,
	add_peer_candidate/1
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

%% ETS table ar_network (belongs to ar_sup process)
%% Peer -> {active|passive, undefined|online|offline}
%% * Peer - {IP,Port}. example {127,0,0,1,1984}
%% * active - peer is able to accept incoming request
%% * passive - peer is behind the NAT.
%% * {online, Pid, Since} - peering has been established. Since - timestamp.
%%   Do disconnect after Since + PEERING_LIFESPAN.
%% * {offline, Since} - do not attempt to connect to this peer
%%   until Since + PEER_SLEEP_TIME.
%% * {undefined, Pid} - just started peering process
%% * undefined - candidate for peering

%% keep this number of joined peers, but do not exceed this limit.
-define(PEERS_JOINED_MAX, 100).
%% do not bother the peer during this time if its went offline
-define(PEER_SLEEP_TIME, 60). % in minutes
%% we should rotate the peers
-define(PEERING_LIFESPAN, 5*60). % in minutes

%% Internal state definition.
-record(state, {
	ready = false,	% switching to 'true' on event {node, ready}
	peering_timer
}).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Send Block to all active(joined) peers. Returns a reference in order to
%% handle {sent, block, Ref} message on a sender side. This message is
%% sending once all the peering process are reported on this request.
%% @spec broadcast(block, Block) -> {ok, Ref} | {error, Error}
%%
%% To - list of peers ID where the given Block should be delivered to.
%% Useful with ar_rating:get_top_joined(N)
%% @spec broadcast(block, Block, To) -> {ok, Ref} | {error, Error}
%% @end
%%--------------------------------------------------------------------
broadcast(block, Block) ->
	gen_server:call(?MODULE, {broadcast, block, Block, self()}).
broadcast(block, Block, To) when is_list(To) ->
	gen_server:call(?MODULE, {broadcast, block, Block, self(), To}).
%% @doc
%% add_peer_candidate adds a record with given information for the
%% future peering
add_peer_candidate(PeerIPPort) ->
	ets:insert(?MODULE, {PeerIPPort, undefined, undefined}).

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
	% subscribe to events:
	%  node - in order to handle {event, node, ready} event and start the peering process
	%  peer - handle {event, peer, {joined,...}}
	ar_events:subscribe([node, peer]),
	NodeJoined = ar_node:is_joined(),
	gen_server:cast(?MODULE, join),
	{ok, #state{ready = NodeJoined}}.

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
	Timer = case peers_joined() of
		[] ->
			{ok, Config} = application:get_env(arweave, config),
			lists:map(fun({A,B,C,D,Port}) ->
				peering({A,B,C,D}, Port, [])
			end, Config#config.peers),
			{ok, T} = timer:send_after(10000, {'$gen_cast', join}),
			T;
		_ when State#state.ready == true->
			% seems its has been restarted. we should
			% start 'peering' process
			{ok, T} = timer:send_after(10000, {'$gen_cast', peering}),
			T;
		_ ->
			% do nothing. waiting for node event 'ready'
			undefined
	end,
	{noreply, State#state{peering_timer = Timer}};
handle_cast(_Msg, State)  when State#state.ready == false ->
	% do nothing
	{noreply, State};
handle_cast(peering, State) ->
	Timer = case peers_joined() of
		[] ->
			gen_server:cast(?MODULE, join),
			undefined;
		Peers when length(Peers) > ?PEERS_JOINED_MAX ->
			{ok, T} = timer:send_after(10000, {'$gen_cast', peering}),
			T;
		_Peers ->
			%N = length(Peers),
			%RatedPeers = ar_rating:get_top(100),


			{ok, T} = timer:send_after(10000, {'$gen_cast', peering}),
			T
	end,
	{noreply, State#state{peering_timer = Timer}};
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
handle_info({event, node, ready}, State) when is_reference(State#state.peering_timer)->
	% already started self casting with 'peering' message.
	{noreply, State};
handle_info({event, node, ready}, State) ->
	gen_server:cast(?MODULE, peering),
	{noreply, State#state{ready = true}};
handle_info({event, node, _}, State) ->
	timer:cancel(State#state.peering_timer),
	% stop any peering if node is not in 'ready' state
	erlang:exit(whereis(ar_network_peer_sup), maintenance),
	{noreply, State#state{ready = false, peering_timer = undefined}};
handle_info({event, peer, {joined, PeerID, IP, Port}}, State) ->
	case ets:lookup(?MODULE, {IP,Port}) of
		[] ->
			?LOG_ERROR("internal error. got event joined from unknown peer",[{IP, Port, PeerID}]),
			{noreply, State};
		[{{IP,Port}, Pid, undefined}] ->
			T = os:system_time(second),
			ets:insert(?MODULE, {{IP,Port}, Pid, T}),
			{noreply, State}
	end;
handle_info({'DOWN', _Ref, process, Pid, Reason}, State) ->
	% peering process is down
	peer_went_offline(Pid, Reason),
	{noreply, State};
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
	?LOG_INFO([{event, ar_network_terminated}, {module, ?MODULE}]),
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
peering(IP, Port, Options)->
	T = os:system_time(second),
	case ets:lookup(?MODULE, {IP,Port}) of
		[] ->
			% first time to connect to this peer
			{ok, Pid} = ar_network_peer_sup:start_peering({IP,Port}, Options),
			ets:insert(?MODULE, {{IP,Port}, Pid, undefined}),
			monitor(process, Pid);
		[{{IP,Port}, undefined, undefined}] ->
			% peer is the candidate. got it from another peer
			{ok, Pid} = ar_network_peer_sup:start_peering({IP,Port}, Options),
			ets:insert(?MODULE, {{IP,Port}, Pid, undefined}),
			monitor(process, Pid);
		[{{IP,Port}, undefined, Since}] when (T - Since)/60 > ?PEER_SLEEP_TIME ->
			% peer was offline, but longer than PEER_SLEEP_TIME
			{ok, Pid} = ar_network_peer_sup:start_peering({IP,Port}, Options),
			ets:insert(?MODULE, {{IP,Port}, Pid, undefined}),
			monitor(process, Pid);
		[{{_IP,_Port}, undefined, Since}] ->
			% peer is offline. let him sleep.
			{offline, Since};
		[{{_IP,_Port}, Pid, _Since}] when is_pid(Pid) ->
			% trying to start peering with the peer we already have peering
			already_started
	end.

peers_joined() ->
	% returns list of {IP,Port}. example: [{{127,0,0,1}1984}]
	ets:select(ar_network, [{{{'$1','$11'},'$2','$3'},[{is_pid, '$2'}],[{{'$1','$11'}}]}]).

peer_went_offline(Pid, Reason) ->
	T = os:system_time(second),
	case ets:match(?MODULE, {'_',Pid,'_'}) of
		[] ->
			unknown;
		[{{IP,Port}, Pid, Since}] ->
			LifeSpan = trunc((T - Since)/60),
			?LOG_INFO("Peer ~p:~p went offline (~p). had peering during ~p minutes", [IP,Port, Reason, LifeSpan]),
			ets:insert(?MODULE, {{IP,Port}, undefined, T})
	end.
