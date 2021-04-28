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
	get_block_shadow/1,
	get_block_index/1,
	get_tx/1,
	get_txs/1,
	get_wallet_list/1,
	get_wallet_list_chunk/1,
	get_wallet_list_chunk/2,

	get_sync_records/0,
	get_chunks/1
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

%% ETS table ar_network (belongs to ar_sup process)
%% {{IP, Port}, Pid, Since}
%% Pid - 'undefined' if peer is offline (candidate or went offline)
%% Since - timestamp of the online/offline statement change

%% do not bother the peer during this time if its went offline
-define(PEER_SLEEP_TIME, 60). % in minutes
%% we should rotate the peers
-define(PEERING_LIFESPAN, 5*60). % in minutes
%%
-define(DEFAULT_REQUEST_TIMEOUT, 10000).
%%
-define(MAX_PARALLEL_REQUESTS, 15).
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
%% Sends Block/TX to the joined peers.
%% @end
%%--------------------------------------------------------------------
broadcast(block, Block) ->
	broadcast(block, Block, {#{}, ?PEERS_JOINED_MAX});
broadcast(tx, TX) ->
	broadcast(tx, TX, {#{}, ?PEERS_JOINED_MAX}).

broadcast(block, Block, {ExcludePeers, Max}) ->
	request({broadcast, ExcludePeers, Max}, {post_block, Block}, 60000);
broadcast(tx, TX, {ExcludePeers, Max}) ->
	request({broadcast, ExcludePeers, Max}, {post_tx, TX}, 60000).

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

%%
%% Methods below are requesting information via the network
%%
get_current_block_index() ->
	request(sequential, {get_block_index, current}, 60000).
get_block_index(Hash) ->
	request(sequential, {get_block_index, Hash}).
get_block_shadow(B) ->
	case request(sequential, {get_block, B}) of
		Block when ?IS_BLOCK(Block), length(Block#block.txs) > ?BLOCK_TX_COUNT_LIMIT ->
			?LOG_ERROR("txs count (~p) exceeds the limit (~p)",
					[length(Block#block.txs), ?BLOCK_TX_COUNT_LIMIT]),
			unavailable;
		Block when ?IS_BLOCK(Block) ->
			Block;
		_ ->
			unavailable
	end.
get_block(B) ->
	case get_block_shadow(B) of
		unavailable ->
			unavailable;
		Block ->
			?LOG_DEBUG("got shadow block ~p. Get txs ~p", [Block#block.height, length(Block#block.txs)]),
			case get_txs(Block#block.txs) of
				unavailable ->
					unavailable;
				{partial, _, _} ->
					% do not handle partial result
					unavailable;
				TXs ->
					Block#block {
						txs = TXs
					}
			end
	end.
get_tx(TX) ->
	get_txs([TX]).
get_txs(TXs) ->
	case request(parallel, {get_tx, TXs}, 30000) of
	timeout ->
		unavailable;
	error ->
		unavailable;
	{nopeers, Result, LeftTXs } ->
		{partial, Result, LeftTXs};
	{timeout, _Result} ->
		unavailable;
	Result ->
		% transform the map into the list
		% keeping the same order we had in TXs
		Reversed = lists:foldl(fun(ID, Acc) ->
			case maps:get(ID, Result, unknown) of
				_ when Acc == error ->
					unavailable;
				unknown ->
					unavailable;
				Tx ->
					[Tx|Acc]
			end
		end, [], TXs),
		% restore the order
		lists:reverse(Reversed)
	end.
get_wallet_list(Hash) ->
	request(sequential, {get_wallet_list, Hash}, 10000).
get_wallet_list_chunk(ID) ->
	request(sequential, {get_wallet_list_chunk, ID}, 10000).
get_wallet_list_chunk(ID, Cursor) ->
	request(sequential, {get_wallet_list_chunk, {ID, Cursor}}, 10000).

get_sync_records() ->
	case request({broadcast, #{}, ?PEERS_JOINED_MAX}, {get_sync_record, []}, 60000) of
		error ->
			unavailable;
		{nopeers, _Result, _Args} ->
			unavailable;
		{timeout, _Result} ->
			unavailavle;
		Result ->
			Result
	end.


% Chunks - is a proplist with value [{PeerID, Offset}|...]
get_chunks(Chunks) ->
	request(affinity, {get_chunk, Chunks}, 60000).

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
handle_cast(join, State) ->
	?LOG_DEBUG("Network joining..."),
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
	?LOG_DEBUG("Network peering..."),
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
			OnlinePeers = lists:foldl(fun({IP,Port,_Pid,_PeerID},M) ->
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
			?LOG_DEBUG("Network peering. Do nothing"),
			prometheus_gauge:set(arweave_peer_count, length(Peers)),
			{ok, T} = timer:send_after(10000, {'$gen_cast', peering}),
			{noreply, State#state{peering_timer = T}}
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
handle_info({event, node, ready}, State) when is_reference(State#state.peering_timer)->
	% already started self casting with 'peering' message.
	{noreply, State};
handle_info({event, node, ready}, State) ->
	?LOG_DEBUG("Network. Start peering..."),
	gen_server:cast(?MODULE, peering),
	%% Now we can serve incoming requests. Start the HTTP server.
	ok = ar_http_iface_server:start(),
	{noreply, State#state{ready = true}};
handle_info({event, node, Reason}, State) ->
	% Reason could be 'worker_terminated', so we should restart the peering
	% and ar_node_worker will be restarted by ar_sup supervisor
	timer:cancel(State#state.peering_timer),
	% stop any peering
	erlang:exit(whereis(ar_network_peer_sup), Reason),
	ar_http_iface_server:stop(),
	{noreply, State#state{ready = false, peering_timer = undefined}};
handle_info({event, peer, {joined, PeerID, IP, Port}}, State) ->
	Joined = peers_joined(),
	case ets:lookup(?MODULE, {IP,Port}) of
		[] ->
			?LOG_ERROR("internal error. got event joined from unknown peer",[{IP, Port, PeerID}]),
			{noreply, State};
		[{{IP,Port}, Pid, undefined, _}] when length(Joined) == 0 ->
			?LOG_DEBUG("Network. First peer ~p:~p joined (~p)", [IP, Port, Pid]),
			ar_events:send(network, connected),
			T = os:system_time(second),
			ets:insert(?MODULE, {{IP,Port}, Pid, T, PeerID}),
			{noreply, State};
		[{{IP,Port}, Pid, undefined, _}] ->
			?LOG_DEBUG("Network. Peer ~p:~p N ~p joined (~p)", [IP, Port, length(Joined), Pid]),
			T = os:system_time(second),
			ets:insert(?MODULE, {{IP,Port}, Pid, T, PeerID}),
			{noreply, State}
	end;
handle_info({event, peer, _}, State) ->
	% ignore any other peer event
	{noreply, State};

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
%%% Internal functions
%%%===================================================================

peering(IP, Port, Options)->
	?LOG_DEBUG("Network. Peering with ~p. Connecting...", [{IP,Port}]),
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
	% returns list of {IP, Port, Pid, PeerID}. example: [{{127,0,0,1}, 1984, <0.12.123>, <<"PeerID">>}]
	% where Pid - is a peering process handler (module: ar_network_peer)
	ets:select(ar_network, [{{{'$1','$11'},'$2','$3','$4'},
							[{is_pid, '$2'}, {'=<', '$3', os:system_time(second)}],
							[{{'$1','$11','$2','$4'}}]}]).
peers_top_joined() ->
	% ordered (by Rating) list of {PeerID, Rating, Host, Port}
	TopJoined = ar_rating:get_top_joined(?PEERS_JOINED_MAX),
	% create a map of joined peers using PeerID => {IP, Port, Pid, PeerID}
	JoinedMap = lists:foldl(fun({IP,Port,Pid,PeerID}, Acc) ->
		maps:put(PeerID, {IP, Port, Pid, PeerID}, Acc)
	end, #{}, peers_joined()),
	% create a list according to the TopJoined order
	Top = lists:foldl(fun({PeerID, _Rating, _Host, _Port}, Acc) ->
		case maps:get(PeerID, JoinedMap, false) of
			false ->
				Acc;
			Peer ->
				[Peer|Acc]
		end
	end, [], TopJoined),
	lists:reverse(Top).


peers_offline() ->
	ets:select(ar_network, [{{{'$1','$11'},'$2','$3','$4'},[{'==', false, {is_pid, '$2'}}],[{{'$1','$11'}}]}]).
peer_went_offline(Pid, Reason) ->
	T = os:system_time(second),
	case ets:match(?MODULE, {'$1',Pid,'$2','$3'}) of
		[[]] ->
			?LOG_ERROR("internal error: unknown pid (peer_went_offline) ~p", [Pid]),
			undefined;
		[[{IP,Port}, undefined, PeerID]] ->
			% couldn't establish peering with this candidate
			ets:insert(?MODULE, {{IP,Port}, undefined, T, PeerID});
		[[{IP,Port}, Since, PeerID]] ->
			LifeSpan = trunc((T - Since)/60),
			?LOG_DEBUG("Network. Peer ~p:~p went offline (~p). Had peering during ~p minutes", [IP,Port, Reason, LifeSpan]),
			ar_events:send(peer, {left, PeerID}),
			ets:insert(?MODULE, {{IP,Port}, undefined, T, undefined})
	end.

% You can make a request in a few different ways:
%	sequential - making request sequentially until the correct answer will be received
%	{sequential, N} - the same, but limit the number of retries
%	broadcast  - broadcasting request and get all answers as a result
%	{broadcast, ExcludePeers, N} - broadcasting request to gather N valid answers excluding given peers
%	parallel   - making request in parallel. Args must be a list.
%
%	returns:
%	   error
%	   {timeout, Result}       - partial data received (for broadcast, parallel requests only)
%	   {nopeers, Result}       - partial data recevied, Args - no result for them.
%	   Result - a list of values for broadcast request
%	            a map of #{Arg => Value} for parallel request
%	            a term - for the sequential one

request(Type, {Request, Args}) ->
	request(Type, {Request, Args}, ?DEFAULT_REQUEST_TIMEOUT).

request(sequential, {Request, Args}, Timeout) ->
	Peers = peers_top_joined(),
	do_request_sequential({Request, Args}, Timeout, Peers);
request({sequential, Max}, {Request, Args}, Timeout) ->
	Peers = lists:sublist(peers_top_joined(), Max),
	do_request_sequential({Request, Args}, Timeout, Peers);

request(broadcast, {Request, Args}, Timeout) ->
	Peers = peers_top_joined(),
	N = length(Peers),
	do_request_broadcast({Request, Args}, N, Timeout, Peers);
request({broadcast, #{}, Max}, {Request, Args}, Timeout) ->
	Peers = lists:sublist(peers_top_joined(), Max),
	N = length(Peers),
	do_request_broadcast({Request, Args}, N, Timeout, Peers);
request({broadcast, ExcludePeers, Max}, {Request, Args}, Timeout) ->
	PeersFiltered = lists:filter(fun(P) ->
		not maps:is_key(P, ExcludePeers)
	end, peers_top_joined()),
	Peers = lists:sublist(PeersFiltered, Max),
	N = length(Peers),
	do_request_broadcast({Request, Args}, N, Timeout, Peers);

request(parallel, {_Request, []}, _Timeout) ->
	[];
request(parallel, {Request, Args}, Timeout) ->
	Peers = peers_top_joined(),
	do_request_parallel({Request, Args}, Timeout, Peers);

% making requests to the given peers. Args must be a proplist
% with value [{PeerID, Arg},...]
request(affinity, {Request, [{_,_}|_] = Args}, Timeout) ->
	do_request_affinity({Request, Args}, Timeout).
%
% do sequential request
%
do_request_sequential({Request, Args}, Timeout, Peers) ->
	Origin = self(),
	PeersWithRandoms = [ {rand:uniform(), P} || P <- Peers ],
	ShuffledPeers = [X || {_,X} <- lists:sort(PeersWithRandoms)],
	R = spawn(fun() -> do_sequential_spawn(Origin, {Request, Args}, Timeout, ShuffledPeers) end),
	receive
		{final, Result} ->
			Result
	after Timeout*3 ->
		exit(R, timeout),
		error
	end.

do_sequential_spawn(Origin, {_Request, Args}, _Timeout, []) ->
	Origin ! {final, {nopeers, Args}};
do_sequential_spawn(Origin, {Request, Args}, Timeout, [Peer|Peers]) ->
	{_IP, _Port, Pid, _PeerID} = Peer,
	?LOG_DEBUG("do_sequential_spawn ~p: ~p", [Peer, {Request, Args}]),
	case catch gen_server:call(Pid, {request, Request, Args}, Timeout) of
		{result, Result} ->
			Origin ! {final, Result};
		not_found ->
			?LOG_DEBUG("do_sequential_spawn not_found (~p)", [{Pid, Request, Args}]),
			do_sequential_spawn(Origin, {Request, Args}, Timeout, Peers);
		E ->
			% should we penalty this peer?
			?LOG_DEBUG("do_sequential_spawn (~p) ~p", [{Pid, Request, Args}, E]),
			do_sequential_spawn(Origin, {Request, Args}, Timeout, Peers)
	end.

%
% do broadcast request
%
do_request_broadcast({Request, Args}, N, Timeout, Peers) ->
	Origin = self(),
	Receiver = spawn(fun() -> do_broadcast_spawn(Origin, {Request, Args}, {0,N,[]}, Timeout, Peers) end),
	receive
		{final, Result} ->
			exit(Receiver, normal),
			Result
	after Timeout*3 ->
		exit(Receiver, timeout),
		error
	end.

do_broadcast_spawn(Origin, {_Request, _Args}, {0,0,Results}, _Timeout, _Peers) ->
	Origin ! {final, Results};
do_broadcast_spawn(Origin, {_Request, Args}, {0,_N,Results}, _Timeout, []) ->
	% no more peers
	Origin ! {final, {nopeers, Results, Args}};
do_broadcast_spawn(Origin, {Request, Args}, {S,N,Results}, Timeout, Peers)
								when S > ?MAX_PARALLEL_REQUESTS; Peers == [] ->
	receive
		error ->
			do_broadcast_spawn(Origin, {Request, Args}, {S-1,N-1,Results}, Timeout, Peers);
		Result ->
			do_broadcast_spawn(Origin, {Request, Args}, {S-1,N-1,[Result|Results]}, Timeout, Peers)
	after Timeout ->
		Origin ! {final, {timeout, Results}}
	end;
do_broadcast_spawn(Origin, {Request, Args}, {S,N,Results}, Timeout, [Peer|Peers]) ->
	Receiver = self(),
	{_IP, _Port, Pid, PeerID} = Peer,
	spawn_link(fun() ->
			case catch gen_server:call(Pid, {request, Request, Args}, Timeout) of
				{result, Result} ->
					Receiver ! {PeerID, Result};
				E ->
					?LOG_DEBUG("do_broadcast_spawn (~p) ~p", [{Pid, Request, Args}, E]),
					Receiver ! error
			end
	end),
	do_broadcast_spawn(Origin, {Request, Args}, {S+1,N,Results}, Timeout, Peers).

%
% do parallel request
%
do_request_parallel({Request, Args}, Timeout, Peers) ->
	Origin = self(),
	Receiver = spawn(fun() -> do_parallel_spawn(Origin, {Request, Args}, {0,#{}}, Timeout, Peers) end),
	receive
		{final, Result} ->
			Result
	after Timeout*3 ->
		exit(Receiver, timeout),
		error
	end.

do_parallel_spawn(Origin, {_Request, []}, {0, Results}, _Timeout, _Peers) ->
	Origin ! {final, Results};
do_parallel_spawn(Origin, {_Request, Args}, {0, Results}, _Timeout, []) ->
	Origin ! {final, {nopeers, Results, Args}};
do_parallel_spawn(Origin, {Request, Args}, {N, Results}, Timeout, Peers)
								when N > ?MAX_PARALLEL_REQUESTS; Args == []; Peers == [] ->
	receive
		{error, A} ->
			% exclude this peer since something wrong with him
			do_parallel_spawn(Origin, {Request, [A|Args]}, {N-1,Results}, Timeout, Peers);
		{not_found, A, Peer} ->
			% Ok, lets get the Arg back to the list for the next attempt.
			% Get back the peer at the end of peers
			do_parallel_spawn(Origin, {Request, [A|Args]}, {N-1,Results}, Timeout, Peers++[Peer]);
		{result, A, Result, Peer} ->
			Results1 = maps:put(A, Result, Results),
			do_parallel_spawn(Origin, {Request, Args}, {N-1, Results1}, Timeout, [Peer|Peers])
	after Timeout*2 ->
		Origin ! {final, {timeout, Results}}
	end;
do_parallel_spawn(Origin, {Request, [A|Args]}, {N, Results}, Timeout, [Peer|Peers]) ->
	Receiver = self(),
	{_IP, _Port, Pid, _PeerID} = Peer,
	spawn_link(fun() ->
			case catch gen_server:call(Pid, {request, Request, A}, Timeout) of
				{result, Result} ->
					?LOG_DEBUG("do_parallel_spawn OK ~p", [{Pid, Request, A}]),
					Receiver ! {result, A, Result, Peer};
				not_found ->
					Receiver ! {not_found, A, Peer};
				E ->
					?LOG_DEBUG("do_parallel_spawn (~p) ~p", [{Pid, Request, A}, E]),
					Receiver ! {error, A}
			end
	end),
	do_parallel_spawn(Origin, {Request, Args}, {N+1, Results}, Timeout, Peers).

%
% do affinity request (binding to a peer)
%
do_request_affinity({Request, Args}, Timeout) ->
	Origin = self(),
	PeersMap = lists:foldl(fun({_IP, _Port, Pid, PeerID}, Acc) ->
		maps:put(PeerID, Pid, Acc)
	end, #{}, peers_joined()),
	Receiver = spawn(fun() -> do_affinity_spawn(Origin, {Request, Args}, {0,#{}}, Timeout, PeersMap) end),
	receive
		{final, Result} ->
			Result
	after Timeout*3 ->
		exit(Receiver, timeout),
		error
	end.

do_affinity_spawn(Origin, {_Request, []}, {0, Results}, _Timeout, _PeersMap) ->
	Origin ! {final, Results};
do_affinity_spawn(Origin, {Request, Args}, {N, Results}, Timeout, PeersMap)
								when N > ?MAX_PARALLEL_REQUESTS; Args == [] ->
	receive
		{error, PeerID} ->
			Results1 = maps:put(PeerID, error, Results),
			do_affinity_spawn(Origin, {Request, Args}, {N-1, Results1}, Timeout, PeersMap);
		{not_found, PeerID} ->
			Results1 = maps:put(PeerID, not_found, Results),
			do_affinity_spawn(Origin, {Request, Args}, {N-1, Results1}, Timeout, PeersMap);
		{result, PeerID, Result} ->
			Results1 = maps:put(PeerID, Result, Results),
			do_affinity_spawn(Origin, {Request, Args}, {N-1, Results1}, Timeout, PeersMap)
	end;

do_affinity_spawn(Origin, {Request, [{PeerID, A} |Args]}, {N, Results}, Timeout, PeersMap) ->
	Receiver = self(),
	Pid = maps:get(PeerID, PeersMap, undefined),
	spawn_link(fun() ->
			case catch gen_server:call(Pid, {request, Request, A}, Timeout) of
				{result, Result} ->
					?LOG_DEBUG("do_affinity_spawn OK ~p", [{Pid, Request, A}]),
					Receiver ! {result, PeerID, Result};
				not_found ->
					Receiver ! {not_found, PeerID};
				E ->
					?LOG_DEBUG("do_affinity_spawn (~p) ~p", [{Pid, Request, A}, E]),
					Receiver ! {error, PeerID}
			end
	end),
	do_affinity_spawn(Origin, {Request, Args}, {N+1, Results}, Timeout, PeersMap).


