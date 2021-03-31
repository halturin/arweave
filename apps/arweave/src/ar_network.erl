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
	add_peer_candidate/2,
	is_connected/0,

	% requests to the peer
	get_current_block_index/0,
	get_block/1,
	get_block_index/1,
	get_wallet_list/1,
	get_wallet_list_chunk/1,
	get_wallet_list_chunk/2
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

%% ETS table ar_network (belongs to ar_sup process)
%% {{IP, Port}, Pid, Since}
%% Pid - 'undefined' if peer is offline (candidate or went offline)
%% Since - timestamp of the online/offline statement change

%% keep this number of joined peers, but do not exceed this limit.
-define(PEERS_JOINED_MAX, 10).
%% do not bother the peer during this time if its went offline
-define(PEER_SLEEP_TIME, 60). % in minutes
%% we should rotate the peers
-define(PEERING_LIFESPAN, 5*60). % in minutes
%%
-define(DEFAULT_REQUEST_TIMEOUT, 10000).
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
%% Sends Block to all active(joined) peers. Returns a reference in order to
%% handle {sent, block, Ref} message on a sender side. This message is
%% sending once all the peering process are reported on this request.
%% @spec broadcast(block, Block) -> {ok, Ref} | {error, Error}
%% @end
%%--------------------------------------------------------------------
broadcast(block, Block) ->
	gen_server:call(?MODULE, {broadcast, block, Block, self()});
broadcast(tx, TX) ->
	gen_server:call(?MODULE, {broadcast, tx, TX, self()}).

%%--------------------------------------------------------------------
%% @doc
%% Sends Block to the given peers. Returns a reference in order to
%% handle {sent, block, Ref} message on a sender side. This message is
%% sending once all the peering process are reported on this request.
%% To - list of peers ID where the given Block should be delivered to.
%% Useful with ar_rating:get_top_joined(N)
%% @spec broadcast(block, Block, To) -> {ok, Ref} | {error, Error}
%% @end
%%--------------------------------------------------------------------
broadcast(block, Block, To) when is_list(To) ->
	gen_server:call(?MODULE, {broadcast, block, Block, self(), To});
broadcast(tx, TX, To) when is_list(To) ->
	gen_server:call(?MODULE, {broadcast, tx, TX, self(), To}).

%% @doc
%% add_peer_candidate adds a record with given IP address and Port number for the
%% future peering. Ignore private networks.
add_peer_candidate({192,168,_,_}, _Port) -> false;
add_peer_candidate({169,254,_,_}, _Port) -> false;
add_peer_candidate({172,X,_,_}, _Port) when X > 15, X < 32 -> false; % 172.16..172.31
add_peer_candidate({10,_,_,_}, _Port) -> false;
add_peer_candidate({127,_,_,_}, _Port) -> false;
add_peer_candidate({255,255,255,255}, _Port) -> false;
add_peer_candidate({A,B,C,D} = IP, Port) when Port > 0, Port < 65536,
											A > 0, A < 256,
											B > -1, B < 256,
											C > -1, C < 256,
											D > -1, D < 256 ->
	% do not overwrite if its already exist
	% {{IP,Port}, Pid, Timestamp, PeerID}
	ets:insert_new(?MODULE, {{IP, Port}, undefined, undefined, undefined});
%% ignore any garbage
add_peer_candidate(_IP, _Port) -> false.

%% @doc
%% Returns whether the node is connected to the network or not.
%% true - has at least one peering connection
%% false - has no peering connections at all
%% @end
is_connected() ->
	length(peers_joined()) > 0.

get_current_block_index() ->
	request({get_block_index, current}, {first, 10}).
get_block_index(Hash) ->
	request({get_block_index, Hash}, {first, 10}).
get_block(B) ->
	case request({get_block, B}, {first, 10}) of
		{shadow, Block} ->
			Block;
		{full, Block} ->
			MempoolTXs = ar_node:get_pending_txs([as_map]),
			case get_txs(MempoolTXs, Block) of
				{ok, TXs} ->
					Block#block {
						txs = TXs
					};
				_ ->
					unavailable
			end;
		_ ->
			unavailable
	end.
get_txs(TXs, B) ->
	request({get_txs, TXs, B}, {first, 10}).
get_wallet_list(Hash) ->
	request({get_wallet_list, Hash}, {first, 10}).
get_wallet_list_chunk(ID) ->
	request({get_wallet_list_chunk, ID}, {first, 10}).
get_wallet_list_chunk(ID, Cursor) ->
	request({get_wallet_list_chunk, {ID, Cursor}}, {first, 10}).
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
	?LOG_ERROR("0000000000000000 joining..."),
	{ok, Config} = application:get_env(arweave, config),
	case peers_joined() of
		Peers when length(Peers) < length(Config#config.peers) ->
			lists:map(fun({A,B,C,D,Port}) ->
				% Override default PEER_SLEEP_TIME for the trusted peers.
				% During the joining we should ignore this timing.
				peering({A,B,C,D}, Port, [{sleep_time, 0}])
			end, Config#config.peers),
			% send some metrics
			prometheus_gauge:set(arweave_peer_count, length(Peers)),
			{ok, T} = timer:send_after(10000, {'$gen_cast', join}),
			{noreply, State#state{peering_timer = T}};
		_ when State#state.ready == true->
			% seems its has been restarted. we should
			% start 'peering' process
			{ok, T} = timer:send_after(10000, {'$gen_cast', peering}),
			{noreply, State#state{peering_timer = T}};
		_ ->
			% do nothing. waiting for node event 'ready'
			{noreply, State#state{peering_timer = undefined}}
	end;
handle_cast(_Msg, State)  when State#state.ready == false ->
	% do nothing
	{noreply, State};
handle_cast(peering, State) ->
	?LOG_ERROR("0000000000000000 peering..."),
	case peers_joined() of
		[] ->
			gen_server:cast(?MODULE, join),
			prometheus_gauge:set(arweave_peer_count, 0),
			{noreply, State#state{peering_timer = undefined}};
		Peers when length(Peers) < ?PEERS_JOINED_MAX ->
			% how many connection we have to have
			N = ?PEERS_JOINED_MAX - length(Peers),
			% get the top 3*PEERS_JOINED_MAX peers and transform it
			% to the [{IP, Port}]
			RatedPeers = lists:foldl(fun({_ID, _Rating, {_,_,_,_} = IP, Port}, L) ->
					[{IP, Port}|L];
				({_ID, _Rating, _Host, _Port}, L) ->
					L
			end, [], ar_rating:get_top(3*?PEERS_JOINED_MAX)),
			% get all offline peers. the same format [{IP, Port}]
			OfflinePeers = peers_offline(),
			% transform list of joined peers into the same format [{IP, Port}]
			OnlinePeers = lists:foldl(fun({IP,Port,_Pid},M) ->
				maps:put({IP,Port},ok,M)
			end, #{}, Peers),
			% merge them all together (excluding already joined peers) into
			% the Candidates list.
			{Candidates,_} = lists:foldl(fun(P,{L,M}) ->
				Skip = maps:get(P,M,false),
				case maps:get(P, OnlinePeers, undefined) of
					undefined when Skip == false ->
						{[P|L],maps:put(P, true, M)};
					_ ->
						{L,M}
				end
			end, {[],#{}}, lists:append(RatedPeers,OfflinePeers) ),
			% get random cadidate N times from Candidates list
			% and start peering process with choosen peer
			lists:foldl(fun(_, []) ->
				% not enough cadidates to reach the limit of PEERS_JOINED_MAX
				[];
			(_, C) ->
				Nth = rand:uniform(length(C)),
				{C1,[{IP,Port}|C2]} = lists:split(Nth - 1, C),
				peering(IP, Port, []),
				lists:append(C1,C2)
			end, Candidates, lists:seq(1,N) ),
			% go further
			prometheus_gauge:set(arweave_peer_count, length(Peers)),
			{ok, T} = timer:send_after(10000, {'$gen_cast', peering}),
			{noreply, State#state{peering_timer = T}};
		Peers ->
			?LOG_ERROR("0000000000000000 do nothing"),
			prometheus_gauge:set(arweave_peer_count, length(Peers)),
			{ok, T} = timer:send_after(10000, {'$gen_cast', peering}),
			{noreply, State#state{peering_timer = T}}
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
handle_info({event, node, ready}, State) when is_reference(State#state.peering_timer)->
	% already started self casting with 'peering' message.
	{noreply, State};
handle_info({event, node, ready}, State) ->
	?LOG_ERROR("0000000000000000 node is ready"),
	gen_server:cast(?MODULE, peering),
	%% Now we can serve incoming requests. Start the HTTP server.
	ok = ar_http_iface_server:start(),
	{noreply, State#state{ready = true}};
handle_info({event, node, _}, State) ->
	timer:cancel(State#state.peering_timer),
	% stop any peering if node is not in 'ready' state
	erlang:exit(whereis(ar_network_peer_sup), maintenance),
	{noreply, State#state{ready = false, peering_timer = undefined}};
handle_info({event, peer, {joined, PeerID, IP, Port}}, State) ->
	Joined = peers_joined(),
	case ets:lookup(?MODULE, {IP,Port}) of
		[] ->
			?LOG_ERROR("internal error. got event joined from unknown peer",[{IP, Port, PeerID}]),
			{noreply, State};
		[{{IP,Port}, Pid, undefined, _}] when length(Joined) == 0 ->
			?LOG_ERROR("0000000000000000 peer FIRST joined "),
			ar_events:send(network, connected),
			T = os:system_time(second),
			ets:insert(?MODULE, {{IP,Port}, Pid, T, PeerID}),
			{noreply, State};
		[{{IP,Port}, Pid, undefined, _}] ->
			?LOG_ERROR("0000000000000000 peer N ~p joined ", [length(Joined)]),
			T = os:system_time(second),
			ets:insert(?MODULE, {{IP,Port}, Pid, T, PeerID}),
			{noreply, State}
	end;
handle_info({'DOWN', _Ref, process, Pid, Reason}, State) ->
	% peering process is down
	peer_went_offline(Pid, Reason),
	case peers_joined() of
		[] ->
			ar_events:send(network, disconnected),
			gen_server:cast(?MODULE, join),
			{noreply, State};
		_ ->
			{noreply, State}
	end;
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
	?LOG_ERROR("0000000000000000 ~p connecting...", [{IP,Port}]),
	T = os:system_time(second),
	SleepTime = proplists:get_value(sleep_time, Options, ?PEER_SLEEP_TIME),
	case ets:lookup(?MODULE, {IP,Port}) of
		[] ->
			% first time connects with this peer
			{ok, Pid} = ar_network_peer_sup:start_peering({IP,Port}, Options),
			ets:insert(?MODULE, {{IP,Port}, Pid, undefined, undefined}),
			monitor(process, Pid);
		[{{IP,Port}, undefined, undefined, undefined}] ->
			% peer is the candidate. got it from another peer
			{ok, Pid} = ar_network_peer_sup:start_peering({IP,Port}, Options),
			ets:insert(?MODULE, {{IP,Port}, Pid, undefined, undefined}),
			monitor(process, Pid);
		[{{IP,Port}, undefined, Since, _PeerID}] when (T - Since)/60 > SleepTime ->
			% peer was offline, but longer than given SleepTime
			{ok, Pid} = ar_network_peer_sup:start_peering({IP,Port}, Options),
			ets:insert(?MODULE, {{IP,Port}, Pid, undefined, undefined}),
			monitor(process, Pid);
		[{{_IP,_Port}, undefined, Since, _PeerID}] ->
			% peer is offline. let him sleep.
			{offline, Since};
		[{{_IP,_Port}, Pid, _Since, _PeerID}] when is_pid(Pid) ->
			% trying to start peering with the peer we already have peering
			already_started
	end.

peers_joined() ->
	% returns list of {IP, Port, Pid}. example: [{{127,0,0,1}, 1984, <0.12.123>}]
	% where Pid - is a peering process handler (module: ar_network_peer)
	ets:select(ar_network, [{{{'$1','$11'},'$2','$3','$4'},
							[{is_pid, '$2'}, {'=<', '$3', os:system_time(second)}],
							[{{'$1','$11', '$2'}}]}]).
peers_offline() ->
	ets:select(ar_network, [{{{'$1','$11'},'$2','$3','$4'},[{'==', false, {is_pid, '$2'}}],[{{'$1','$11'}}]}]).
peer_went_offline(Pid, Reason) ->
	T = os:system_time(second),
	case ets:match(?MODULE, {'$1',Pid,'$2','$3'}) of
		[[]] ->
			?LOG_ERROR("internal error: unknown pid (peer_went_offline) ~p", [Pid]),
			undefined;
		[[{IP,Port}, undefined, _PeerID]] ->
			% couldn't establish peering with this candidate
			ets:insert(?MODULE, {{IP,Port}, undefined, T});
		[[{IP,Port}, Since, PeerID]] ->
			LifeSpan = trunc((T - Since)/60),
			?LOG_INFO("Peer ~p:~p went offline (~p). Had peering during ~p minutes", [IP,Port, Reason, LifeSpan]),
			ar_events:send(peer, {left, PeerID}),
			ets:insert(?MODULE, {{IP,Port}, undefined, T, undefined})
	end.

% You can make a request in a few different ways:
%	sequential - making request sequentially until the correct answer will be received
%	{sequential, N} - the same, but limit the number of retries
%	broadcast  - broadcasting request and get all answers as a result
%	{broadcast, N} - broadcasting request to gather N valid answers
%	parallel   - making request in parallel. Args must be a list.

request(Type, {Request, Args}) ->
	request(Type, {Request, Args}, ?DEFAULT_REQUEST_TIMEOUT).

request(sequential, {Request, Args}, Timeout) ->
	Peers = peers_joined(),
	do_request_sequential({Request, Args}, Timeout, Peers);
request({sequential, Max}, {Request, Args}, Timeout) ->
	Peers = lists:sublist(peers_joined(), Max),
	do_request_sequential({Request, Args}, Timeout, Peers);

request(broadcast, {Request, Args}, Timeout) ->
	Peers = peers_joined(),
	N = length(Peers),
	do_request_broadcast({Request, Args}, N, Timeout, Peers);
request({broadcast, N}, {Request, Args}, Timeout) ->
	Peers = peers_joined(),
	do_request_broadcast({Request, Args}, N, Timeout, Peers);

request(parallel, {Request, [A|Args]}, Timeout) ->
	Peers = peers_joined(),
	do_request_parallel({Request, [A|Args]}, Timeout, Peers).

%
% do sequential request
%
do_request_sequential({Request, Args}, Timeout, Peers) ->
	Origin = self(),
	R = spawn(fun() -> do_sequential_spawn(Origin, {Request, Args}, Timeout, Peers) end),
	receive
		Result ->
			Result
	after Timeout*3 ->
		exit(R, timeout),
		timeout
	end.

do_sequential_spawn(Origin, {_Request, _Args}, _Timeout, []) ->
	Origin ! error;
do_sequential_spawn(Origin, {Request, Args}, Timeout, [{_IP, _Port, Pid}|Peers]) ->
	case catch gen_server:call(Pid, {request, Request, Args}, Timeout) of
		{result, Result} ->
			Origin ! Result;
		E ->
			?LOG_ERROR("AAAAAAAAAAAAAAAAA2 (~p) ~p", [{Pid, Request, Args}, E]),
			do_sequential_spawn(Origin, {Request, Args}, Timeout, Peers)
	end.

%
% do broadcast request
%
do_request_broadcast({Request, Args}, N, Timeout, Peers) ->
	Origin = self(),
	Receiver = spawn(fun() -> do_broadcast_spawn(Origin, {Request, Args}, {N,N,[]}, Timeout, Peers) end),
	receive
		Result ->
		exit(Receiver, normal),
			Result
	after Timeout*3 ->
		exit(Receiver, timeout),
		timeout
	end.

do_broadcast_spawn(Origin, {_Request, _Args}, {_,_,Results}, _Timeout, []) ->
	% no more peers
	Origin ! {nopeers, Results};
do_broadcast_spawn(Origin, {_Request, _Args}, {0,0,Results}, _Timeout, _Peers) ->
	Origin ! Results;
do_broadcast_spawn(Origin, {Request, Args}, {0,N,Results}, Timeout, Peers) ->
	receive
		error ->
			do_broadcast_spawn(Origin, {Request, Args}, {1,N,Results}, Timeout, Peers);
		Result ->
			do_broadcast_spawn(Origin, {Request, Args}, {0,N-1,[Result|Results]}, Timeout, Peers)
	after Timeout ->
		Origin ! {timeout, Results}
	end;
do_broadcast_spawn(Origin, {Request, Args}, {S,N,Results}, Timeout, [{_IP, _Port, Pid}|Peers]) ->
	Receiver = self(),
	spawn_link(fun() ->
			case catch gen_server:call(Pid, {request, Request, Args}, Timeout) of
				{result, Result} ->
					Receiver ! Result;
				E ->
					?LOG_ERROR("AAAAAAAAAAAAAAAAA3 (~p) ~p", [{Pid, Request, Args}, E]),
					Receiver ! error
			end
	end),
	do_broadcast_spawn(Origin, {Request, Args}, Peers, {S-1,N,Results}, Timeout).

%
% do parallel request
%
do_request_parallel({Request, Args}, Timeout, Peers) ->
	Origin = self(),
	Receiver = spawn(fun() -> do_parallel_spawn(Origin, {Request, Args}, {0,[]}, Timeout, Peers) end),
	receive
		Result ->
			Result
	after Timeout*3 ->
		exit(Receiver, timeout),
		timeout
	end.

do_parallel_spawn(Origin, {_Request, []}, {0, Results}, _Timeout, _Peers) ->
	Origin ! Results;
do_parallel_spawn(Origin, {Request, Args}, {N, Results}, Timeout, Peers) when Args == []; Peers == [] ->
	receive
		{error, A} ->
			do_parallel_spawn(Origin, {Request, [A]}, {N,Results}, Timeout, Peers);
		{result, Result, Peer} ->
			do_parallel_spawn(Origin, {Request, []}, {N-1, [Result|Results]}, Timeout, [Peer|Peers])
	after Timeout*2 ->
		Origin ! {timeout, Results}
	end;
do_parallel_spawn(Origin, {Request, [A|Args]}, {N, Results}, Timeout, [{IP, Port, Pid}|Peers]) ->
	Receiver = self(),
	spawn_link(fun() ->
			case catch gen_server:call(Pid, {request, Request, A}, Timeout) of
				{result, Result} ->
					Receiver ! {result, Result, {IP, Port, Pid}};
				E ->
					?LOG_ERROR("AAAAAAAAAAAAAAAAA4 (~p) ~p", [{Pid, Request, A}, E]),
					Receiver ! {error, A}
			end
	end),
	do_parallel_spawn(Origin, {Request, Args}, {N+1, Results}, Timeout, Peers).

