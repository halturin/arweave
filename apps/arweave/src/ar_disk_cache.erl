%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_disk_cache).

-behaviour(gen_server).

-export([
	lookup_block_filename/1,
	lookup_tx_filename/1,
	reset/0
]).

-export([
	start_link/0,
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-define(CACHE_DIR, "cache/").
-define(CACHE_BLOCK_DIR, "block").
-define(CACHE_TX_DIR, "tx").

%% Internal state definition.
-record(state, {
	limit_max,
	limit_min,
	size = 0,
	path,
	block_path,
	tx_path
}).

%%%===================================================================
%%% API
%%%===================================================================

lookup_block_filename(Hash) when is_binary(Hash)->
	% use the process dictionary to keep the path
	PathBlock = case get(ar_disk_cache_path) of
		undefined ->
			{ok, Config} = application:get_env(arweave, config),
			Path = filename:join(Config#config.data_dir, ?CACHE_DIR),
			put(ar_disk_cach_path, Path),
			filename:join(Path, ?CACHE_BLOCK_DIR);
		Path ->
			filename:join(Path, ?CACHE_BLOCK_DIR)
	end,
	FileName = binary_to_list(Hash) ++ ".json",
	File = filename:join(PathBlock, FileName),
	case filelib:is_file(File) of
		true ->
			File;
		_ ->
			unavailable
	end.

lookup_tx_filename(Hash) when is_binary(Hash) ->
	PathTX = case get(ar_disk_cache_path) of
		undefined ->
			{ok, Config} = application:get_env(arweave, config),
			Path = filename:join(Config#config.data_dir, ?CACHE_DIR),
			put(ar_disk_cach_path, Path),
			filename:join(Path, ?CACHE_TX_DIR);
		Path ->
			filename:join(Path, ?CACHE_TX_DIR)
	end,
	FileName = binary_to_list(Hash) ++ ".json",
	File = filename:join(PathTX, FileName),
	case filelib:is_file(File) of
		true ->
			File;
		_ ->
			unavailable
	end.

reset() ->
	gen_server:call(?MODULE, reset).
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
	{ok, Config} = application:get_env(arweave, config),
	ar_events:subscribe([tx,block]),
	Path = filename:join(Config#config.data_dir, ?CACHE_DIR),
	BlockPath = filename:join(Path, ?CACHE_BLOCK_DIR),
	TXPath = filename:join(Path, ?CACHE_TX_DIR),

	ok = filelib:ensure_dir(BlockPath ++ "/"),
	ok = filelib:ensure_dir(TXPath ++ "/"),
	Size = filelib:fold_files(Path, ".*\\.json$", true, fun(F,Acc) -> filelib:file_size(F)+Acc end, 0),
	LimitMax = Config#config.disk_cache_size * 1048576, % MB to Bytes,
	LimitMin = trunc(LimitMax * (100 - ?DISK_CACHE_CLEAN_PERCENT_MAX)/100),
	State = #state{
		limit_max = LimitMax,
		limit_min = LimitMin,
		size = Size,
		path = Path,
		block_path = BlockPath,
		tx_path = TXPath
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
handle_call(reset, _From, State) ->
	Path = State#state.path,
	?LOG_DEBUG("reset disk cache: ~p", [Path]),
	os:cmd("rm -r " ++ Path ++ "/*"),
	BlockPath = filename:join(Path, ?CACHE_BLOCK_DIR),
	TXPath = filename:join(Path, ?CACHE_TX_DIR),
	ok = filelib:ensure_dir(BlockPath ++ "/"),
	ok = filelib:ensure_dir(TXPath ++ "/"),
	{reply, ok, State#state{size = 0}};

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
handle_cast(check_clean, State) when State#state.size > State#state.limit_max ->
	?LOG_DEBUG("Exceed the limit (~p): ~p", [State#state.limit_max, State#state.size]),
	Files = lists:sort(filelib:fold_files(
		State#state.path,
		".*\\.json$",
		true,
		fun(F,A) ->
			 [{filelib:last_modified(F), filelib:file_size(F), F}|A]
		end,
		[])),
	% how much space should be cleaned up.
	CleanSize = State#state.size - State#state.limit_min,
	X = delete_file(Files, CleanSize),
	Size = State#state.size - (CleanSize-X),
	?LOG_DEBUG("Cleaned space ~p bytes. Current size: ~p", [ CleanSize - X, Size ]),
	{noreply, State#state{size = Size}};

handle_cast(check_clean, State) ->
	{noreply, State};

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
handle_info({event, block, {new, Block, _FromPeerID}}, State) ->
	% ignore mined and keep the new one only.
	Name = binary_to_list(ar_util:encode(Block#block.indep_hash)) ++ ".json",
	File = filename:join(State#state.block_path, Name),
	JSONStruct = ar_serialize:block_to_json_struct(Block),
	Data = ar_serialize:jsonify(JSONStruct),
	Size = State#state.size + byte_size(Data),
	case file:write_file(File, Data) of
		ok ->
			?LOG_DEBUG("Added block to cache. file (~p bytes) ~p. Cache size: ~p", [byte_size(Data), Name, Size]),
			gen_server:cast(?MODULE, check_clean),
			{noreply, State#state{size = Size}};
		{error, Reason} ->
			?LOG_ERROR("Can't add block to cache. File: ~p. Reason: ~p", [Name, Reason]),
			{noreply, State}
	end;

handle_info({event, block, _}, State) ->
	{noreply, State};

handle_info({event, tx, {new, TX, _FromPeerID}}, State) ->
	% keep the new TX only
	Name = binary_to_list(ar_util:encode(TX#tx.id)) ++ ".json",
	File = filename:join(State#state.tx_path, Name),
	JSONStruct = ar_serialize:tx_to_json_struct(TX),
	Data = ar_serialize:jsonify(JSONStruct),
	Size = State#state.size + byte_size(Data),
	case file:write_file(File, Data) of
		ok ->
			?LOG_DEBUG("Added tx to cache. file (~p bytes) ~p. Cache size: ~p", [byte_size(Data), Name, Size]),
			gen_server:cast(?MODULE, check_clean),
			{noreply, State#state{size = Size}};
		{error, Reason} ->
			?LOG_ERROR("Can't add tx to cache. File: ~p. Reason: ~p", [Name, Reason]),
			{noreply, State}
	end;

handle_info({event, tx, _}, State) ->
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {message, Info}]),
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
delete_file([], CleanSize) ->
	CleanSize;
delete_file(_Files, CleanSize) when CleanSize < 0 ->
	CleanSize;
delete_file([{_DateTime, Size, Filename} | Files], CleanSize) ->
	case file:delete(Filename) of
		ok ->
			?LOG_DEBUG("Clean disk cache. File (~p bytes): ~p", [Size, Filename]),
			delete_file(Files, CleanSize - Size);
		{error, Reason} ->
			?LOG_ERROR("Can't delete file ~p: ~p", [Filename, Reason]),
			delete_file(Files, CleanSize)
	end.
