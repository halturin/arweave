-module(ar_storage1).

-behavior(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0]).
-export([start_link/1]).

-export([init/1,
		handle_continue/2,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
		terminate/2
		]).

-export([put/3, get/2, delete/2, delete_range/3 ]).

%% @doc Directory for RocksDB key-value storages, relative to the data dir.
-define(DEFAULT_DATA_DIR, "data1").

-record(state, {
	dbs = #{}
}).

%% API

put(TableName, Key, Value) ->
	%% We use the calling process dictionary for caching the value of DB and CF
	%% in order to get rid of gen_server call and work with RocksDB directly, just
	%% as a library call.
	case erlang:get(TableName) of
		undefined ->
			%% This is the first call from the calling process so
			%% get the value of DB[,CF] and cache them into the its
			%% own process dictionary.
			case gen_server:call(?MODULE, {get_db, TableName}) of
				{db, {DB,CF,Files}} ->
					erlang:put(TableName, {DB,CF,Files}),
					rocksdb:put(DB, CF, Key, Value, []);
				{db, {DB,Files}} ->
					erlang:put(TableName, {DB,Files}),
					rocksdb:put(DB, Key, Value, []);
				_ ->
					error
			end;

		{DB,CF,_} ->
			try
				rocksdb:put(DB, CF, Key, Value, [])
			catch _:_ ->
				%% If this gen_server has restarted and database was reopened
				%% we should update cached value of DB and CF in the calling
				%% process dictionary.
				erlang:erase(TableName),
				put(TableName, Key, Value)
			end;
		{DB,_} ->
			try
				rocksdb:put(DB, Key, Value, [])
			catch _:_ ->
				erlang:erase(TableName),
				put(TableName, Key, Value)
			end
	end.

get(TableName, Key) ->
	case erlang:get(TableName) of
		undefined ->
			case gen_server:call(?MODULE, {get_db, TableName}) of
				{db, {DB,CF,undefined}} ->
					erlang:put(TableName, {DB,CF,undefined}),
					rocksdb:get(DB, CF, Key, []);
				{db, {DB,CF,Files}} ->
					erlang:put(TableName, {DB,CF,Files}),
					case rocksdb:get(DB, CF, Key, []) of
						not_found ->
							read_from_file(filename:join(Files,Key));
						Value ->
							Value % {ok, Value}.
					end;
				{db, {DB, undefined}} ->
					erlang:put(TableName, {DB,undefined}),
					rocksdb:get(DB, Key, []);
				{db, {DB, Files}} ->
					erlang:put(TableName, {DB,Files}),
					case rocksdb:get(DB, Key, []) of
						not_found ->
							read_from_file(filename:join(Files,Key));
						Value ->
							Value % {ok, Value}.
					end;
				_ ->
					error
			end;
		{DB,CF,undefined} ->
			try
				rocksdb:get(DB, CF, Key, [])
			catch _:_ ->
				erlang:erase(TableName),
				get(TableName, Key)
			end;
		{DB,CF,Files} ->
			try
				case rocksdb:get(DB, CF, Key, []) of
					not_found ->
						read_from_file(filename:join(Files,Key));
					Value ->
						Value % {ok, Value}.
				end
			catch _:_ ->
				erlang:erase(TableName),
				get(TableName, Key)
			end;
		{DB,undefined} ->
			try
				rocksdb:get(DB, Key, [])
			catch _:_ ->
				erlang:erase(TableName),
				get(TableName, Key)
			end;
		{DB,Files} ->
			try
				case rocksdb:get(DB, Key, []) of
					not_found ->
						read_from_file(filename:join(Files,Key));
					Value ->
						Value % {ok, Value}.
				end
			catch _:_ ->
				erlang:erase(TableName),
				get(TableName, Key)
			end
	end.

delete(TableName, Key) ->
	case erlang:get(TableName) of
		undefined ->
			case gen_server:call(?MODULE, {get_db, TableName}) of
				{ok, {DB,CF,Files}} ->
					erlang:put(TableName, {DB,CF,Files}),
					rocksdb:delete(DB, CF, Key, []);
				{ok, {DB,Files}} ->
					erlang:put(TableName, {DB,Files}),
					rocksdb:delete(DB, Key, []);
				_ ->
					error
			end;
		{DB,CF,_Files} ->
			try
				rocksdb:delete(DB, CF, Key, [])
			catch _:_ ->
				erlang:erase(TableName),
				delete(TableName, Key)
			end;
		{DB,_Files} ->
			try
				rocksdb:delete(DB, Key, [])
			catch _:_ ->
				erlang:erase(TableName),
				delete(TableName, Key)
			end
	end.

delete_range(TableName, StartKey, EndKey) ->
	case erlang:get(TableName) of
		undefined ->
			case gen_server:call(?MODULE, {get_db, TableName}) of
				{db, {DB,CF,Files}} ->
					erlang:put(TableName, {DB,CF,Files}),
					rocksdb:delete_range(DB, CF, StartKey, EndKey, []);
				{db, {DB,Files}} ->
					erlang:put(TableName, {DB,Files}),
					rocksdb:delete_range(DB, StartKey, EndKey, []);
				_ ->
					error
			end;
		{DB,CF,_Files} ->
			try
				rocksdb:delete_range(DB, CF, StartKey, EndKey, [])
			catch _:_ ->
				erlang:erase(TableName),
				delete_range(TableName, StartKey, EndKey)
			end;
		{DB,_Files} ->
			try
				rocksdb:delete_range(DB, StartKey, EndKey, [])
			catch _:_ ->
				erlang:erase(TableName),
				delete_range(TableName, StartKey, EndKey)
			end
	end.

% GenServer callbacks

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, ar_storage_names:list(), []).

start_link(DBList) when is_list(DBList) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, DBList, []).

init(DBList) ->
	process_flag(trap_exit, true),
	{ok, #state{}, {continue, {init_storage, DBList} } }.

handle_continue({init_storage, DBList}, State) ->
	%% Open all databases and merge theirs references into the single map
	%% with value {Name,CF} => {RefName, RefCFname} - for CF db
	%%            Name => RefName - for the regular db
	DBS = lists:foldl(
		fun(DB, Acc) ->
				% 'open' returns a map with DB reference
				D = open(DB, true),
				maps:merge(Acc, D)
		end,
		#{}, % Acc
		DBList),
	{noreply, State#state{dbs = DBS}}.

handle_call({get_db, TableName}, _From, State) ->
	case maps:get(TableName, State#state.dbs, undefined) of
		undefined ->
			{reply, error, State};
		DB ->
			{reply, {db, DB}, State}
	end;

handle_call(_Message, _From, State) ->
	?LOG_ERROR("unhanled call"),
	{reply, ok, State}.

handle_cast(_Message, State) ->
	?LOG_ERROR("unhanled cast"),
	{noreply, State}.

handle_info(_Message, State) ->
	?LOG_ERROR("unhanled info"),
	{noreply, State}.

terminate(_Reason, State) ->
	%% We need the list of unique DB refs only (to exclude multiple DB values
	%% due to {DB, CFrefs}).
	DBS = lists:foldl(fun({DB,_CF,_Files}, Acc) ->
							Acc#{DB => ok};
						({DB,_Files}, Acc) ->
							Acc#{DB => ok}
					end, #{}, maps:values(State#state.dbs)),
	lists:map(fun rocksdb:close/1, maps:keys(DBS)),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

open({Name, Opts}, Repair) ->
	NameConverted = atom_to_list(Name),
	DataDir = application:get_env(arweave, data_dir, ?DEFAULT_DATA_DIR),
	Filename = filename:join(DataDir, NameConverted),
	ok = filelib:ensure_dir(Filename ++ "/"),
	Files = proplists:get_value(files, Opts, undefined),
	case rocksdb:open(Filename, Opts) of
		{ok, DB} ->
			#{Name => {DB, Files}};
		_Error when Repair == true ->
			rocksdb:repair(Filename, []),
			open(Name, false);
		Error ->
			?LOG_ERROR("Can't open DB ~p:~p", [Name, Error]),
			#{}
	end;

open({Name, CFOpts, Opts}, Repair) ->
	%% Convert atoms into the strings (lists).
	CFOptsConverted = lists:map(fun({K,V}) -> {atom_to_list(K),V} end, CFOpts),
	NameConverted = atom_to_list(Name),
	DataDir = application:get_env(arweave, data_dir, ?DEFAULT_DATA_DIR),
	Filename = filename:join(DataDir, NameConverted),
	ok = filelib:ensure_dir(Filename ++ "/"),
	Files = proplists:get_value(files, Opts, undefined),
	case rocksdb:open(Filename, Opts, CFOptsConverted) of
		{ok, DB, CFs} ->
			%% Transform result into the map:
			%% {Name, CFname} => {NameRef, CFnameRef, Files}
			%% Here is how this transformation works:
			%% lists:mapfoldl(
			%% 	fun(K,{M, [A|Acc]}) ->
			%% 		{K, {M#{K => A}, Acc}}
			%% 	end,
			%% 	{#{},[8,7,6,5]},
			%% 	[a,b,c,d])
			%% Result: { _ , { #{a => 8,b => 7,c => 6,d => 5} , _ } }
			{_, {DBS, _}} = lists:mapfoldl(
				fun({CFname, _}, {D, [C|CF]}) ->
					{CFname, {D#{{Name, CFname} => {DB,C,Files}}, CF}}
				end,
				{#{}, CFs}, % Acc
				CFOpts),
			DBS;
		_Error when Repair == true->
			%% Try to repair DB file if its possible and
			%% do another attepmt to open it.
			rocksdb:repair(Filename, []),
			open({Name, CFOpts, Opts},false);
		Error ->
			?LOG_ERROR("Can't open DB ~p:~p", [Name, Error]),
			#{}
	end.

read_from_file(File) ->
	case file:read_file(File) of
		{error, _} ->
			not_found;
		Value ->
			Value % {ok, Value}.
	end.

