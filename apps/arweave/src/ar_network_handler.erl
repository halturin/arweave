
%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_network_handler).

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

-export([
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {
	peer_id,
	router = false,
	handlers = #{},
	handler_rr = 2
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
init({I, Options}) ->
	process_flag(trap_exit, true),
	ID = maps:get(peer_id, Options, common),
	case ets:insert_new(?MODULE, {ID, self()}) of
		true ->
			{ok, #state{ peer_id = ID, router = true }};
		false ->
			[{ID,Router}] = ets:lookup(?MODULE, ID),
			gen_server:cast(Router, {add_handler, I, self()}),
			{ok, #state{ peer_id = ID }}
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

%% Router handler (if this child act as a router - was a first child)
handle_call({handle, Request}, From, State) when State#state.router == true ->
	case {State#state.handler_rr, length(State#state.handlers)} of
		{_,0} ->
			{reply, no_handler, State};
		{N,L} when N < L ->
			Handler = maps:get(N, State#state.handlers),
			erlang:send(Handler, {'$gen_call', From, {handle, Request}}),
			{noreply, State#state{handler_rr = N+1}};
		{_,_} ->
			Handler = maps:get(0, State#state.handlers),
			erlang:send(Handler, {'$gen_call', From, {handle, Request}}),
			{noreply, State#state{handler_rr = 2}}
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
handle_cast({add_handler, I, Pid}, State) ->
	Handlers = maps:put(I, Pid, State#state.handlers),
	{noreply, State#state{handlers = Handlers}};
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
terminate(_Reason, State) when State#state.router == true ->
	ets:delete(?MODULE, State#state.peer_id),
	ok;
terminate(_Reason, _State) ->
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
