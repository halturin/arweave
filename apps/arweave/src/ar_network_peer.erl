%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_network_peer).

-behaviour(gen_server).

-export([
	start_link/1
]).

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
-include_lib("arweave/include/ar_rating.hrl").

%% Internal state definition.
-record(state, {
	since = os:system_time(second),
	lifespan,
	id, % my id
	peer_ipport, % {A,B,C,D,E} where {A,B,C,D} - IP address, E - port
	peer_ip,
	peer_port,
	peer_id,
	peers = #{}, % map of peers
	validate_time = true,
	fails = 0,
	module = ar_network_http_client % default mode is stateless
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
start_link(Args) ->
	gen_server:start_link(?MODULE, Args, []).

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
init({{IP,Port}, Options}) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	ValidateTime = lists:member(time_syncing, Config#config.disable),
	{A,B,C,D} = IP,
	PeerIPPort = {A,B,C,D,Port},
	gen_server:cast(self(), validate_network),
	Now = os:system_time(second),
	State = #state {
		validate_time = ValidateTime,
		peer_ipport = PeerIPPort,
		peer_ip = IP,
		peer_port = Port,
		%lifespan = Now + proplists:get_value(lifespan, Options,?PEERING_LIFESPAN) * 60
		lifespan = Now + proplists:get_value(lifespan, Options, 2) * 60
	},
	{ok, State}.

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
handle_call({request, Request, Args}, _From, State) when is_atom(Request) ->
	T = os:system_time(millisecond),
	case catch (State#state.module):Request(State#state.peer_ipport, Args) of
		{ok, Value} ->
			EventType = get_event_type(Request),
			Event = #event_peer{
				peer = State#state.peer_id,
				time = os:system_time(millisecond) - T
			},
			ar_events:send(peer, {response, EventType, Event}),
			{reply, {result, Value}, State};
		{error, timeout} ->
			Event = #event_peer{
				peer = State#state.peer_id
			},
			ar_events:send(peer, {response, request_timeout, Event}),
			{reply, timeout, State};
		Error ->
			% seems like malformed response. we couldn't serialize from JSON
			Event = #event_peer{
				peer = State#state.peer_id
			},
			ar_events:send(peer, {response, malformed, Event}),
			?LOG_ERROR("Call '~p:~p'(peer id:~p) failed: ~p",
					   [State#state.module, Request, State#state.peer_id, Error]),
			{reply, error, State}
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
handle_cast(_Msg, State) when State#state.fails > 3 ->
	{stop, normal, State};

handle_cast(validate_network, State) ->
	case (State#state.module):get_info(State#state.peer_ipport, {}) of
		Info when is_list(Info) ->
			validate_network(Info,State);
		_ ->
			timer:send_after(5000, {'$gen_cast', validate_network}),
			{noreply, State#state{fails = State#state.fails + 1}}
	end;

handle_cast(validate_time, State) when State#state.validate_time == false ->
	Joined = {joined,
		State#state.peer_id,
		State#state.peer_ip,
		State#state.peer_port
	 },
	ar_events:send(peer, Joined),
	?LOG_ERROR("0000000000000000 ~p just joined", [State#state.peer_ipport]),
	gen_server:cast(self(), get_peers),
	Args = #{
		peer_id => State#state.peer_id,
		peer_ip => State#state.peer_ip,
		peer_port => State#state.peer_port
	},
	ar_network_handler_sup:start_link(3, Args),
	{noreply, State#state{fails = 0}};

handle_cast(validate_time, State) ->
	case (State#state.module):get_time(State#state.peer_ipport) of
		{ok, {RemoteTMin, RemoteTMax}} ->
			LocalT = os:system_time(second),
			Tolerance = ?JOIN_CLOCK_TOLERANCE,
			case LocalT of
				T when T < RemoteTMin - Tolerance ->
					log_peer_clock_diff(State#state.peer_ipport, RemoteTMin - Tolerance - T),
					{stop, normal, State};
				T when T < RemoteTMin - Tolerance div 2 ->
					log_peer_clock_diff(State#state.peer_ipport, RemoteTMin - T),
					gen_server:cast(self(), validate_time),
					{noreply, State#state{validate_time = false}};
				T when T > RemoteTMax + Tolerance ->
					log_peer_clock_diff(State#state.peer_ipport, T - RemoteTMax - Tolerance),
					{stop, normal, State};
				T when T > RemoteTMax + Tolerance div 2 ->
					log_peer_clock_diff(State#state.peer_ipport, T - RemoteTMax),
					gen_server:cast(self(), validate_time),
					{noreply, State#state{validate_time = false}};
				_ ->
					gen_server:cast(self(), validate_time),
					{noreply, State#state{validate_time = false}}
			end;
		error ->
			ar:console(
				"Failed to get time from peer ~s",
				[ar_util:format_peer(State#state.peer_ipport)]
			),
			timer:send_after(5000, {'$gen_cast', validate_time}),
			{noreply, State#state{fails = State#state.fails + 1}}
	end;

handle_cast(get_peers, State) ->
	timer:send_after(60000, {'$gen_cast', get_peers}),
	Now = os:system_time(second),
	case ar_network_http_client:get_peers(State#state.peer_ipport, {}) of
		_Peers when State#state.lifespan < Now ->
			?LOG_ERROR("0000000000000000 ~p time to die", [State#state.peer_ipport]),
			{stop, normal, State};
		Peers when is_list(Peers) ->
			lists:map(fun({A,B,C,D,Port}) ->
				ar_network:add_peer_candidate({A,B,C,D}, Port)
			end, Peers),
			{noreply, State};
		_ ->
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
validate_network(Info, State) ->
	PeerID = list_to_binary(lists:flatten(io_lib:format("~p",[State#state.peer_ipport]))),
	case proplists:get_value(name, Info) of
		<<?NETWORK_NAME>> ->
			State1 = State#state{
				fails = 0,
				peer_id = proplists:get_value(id, Info, PeerID)
			},
			gen_server:cast(self(), validate_time),
			{noreply, State1};
		_ ->
			{stop, normal, State}
	end.

log_peer_clock_diff(Peer, Diff) ->
	Warning = "Your local clock deviates from peer ~s by ~B seconds or more.",
	WarningArgs = [ar_util:format_peer(Peer), Diff],
	io:format(Warning, WarningArgs),
	?LOG_WARNING(Warning, WarningArgs).

% transform Request method into the event name in order to rate this action
% for the peer
get_event_type(get_block) -> block;
get_event_type(get_tx) -> tx;
get_event_type(get_chunk) -> chunk;
get_event_type(_Request) -> unknown.

