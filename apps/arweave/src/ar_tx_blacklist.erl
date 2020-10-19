%%% @doc The module manages a transaction blacklist. The blacklisted identifiers
%%% are read from the configured files or downloaded from the configured HTTP endpoints.
%%% The server coordinates the removal of the transaction headers and data and answers
%%% queries about the currently blacklisted transactions and the corresponding global
%%% byte offsets.
%%% @end
-module(ar_tx_blacklist).

-behaviour(gen_server).

-export([
	start_link/0,
	is_tx_blacklisted/1,
	is_byte_blacklisted/1,
	notify_about_removed_tx/1,
	notify_about_removed_tx_data/1,
	store_state/0
]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-include_lib("arweave/include/ar.hrl").

%% @doc The frequency of refreshing the blacklist.
-ifdef(DEBUG).
-define(REFRESH_BLACKLISTS_FREQUENCY_MS, 2000).
-else.
-define(REFRESH_BLACKLISTS_FREQUENCY_MS, 60 * 60 * 1000).
-endif.

%% @doc How long to wait before retrying to compose a blacklist from local and external
%% sources after a failed attempt.
-define(REFRESH_BLACKLISTS_RETRY_DELAY_MS, 10000).

%% @doc How long to wait for the response to the previously requested
%% header or data removal (takedown) before requesting it for a new tx.
%% @end
-define(REQUEST_TAKEDOWN_DELAY_MS, 2000).

%% @doc The frequency of checking whether the time for the response to
%% the previously requested takedown is due.
%% @end
-define(CHECK_PENDING_ITEMS_INTERVAL_MS, 1000).

%% @doc The frequency of persisting the server state.
-ifdef(DEBUG).
-define(STORE_STATE_FREQUENCY_MS, 20000).
-else.
-define(STORE_STATE_FREQUENCY_MS, 10 * 60 * 1000).
-endif.

%% @doc The server state.
-record(ar_tx_blacklist_state, {
	%% @doc The timestamp of the last requested transaction header takedown.
	%% It is used to throttle the takedown requests.
	%% @end.
	header_takedown_request_timestamp = os:system_time(millisecond),
	%% @doc The timestamp of the last requested transaction data takedown.
	%% It is used to throttle the takedown requests.
	%% @end.
	data_takedown_request_timestamp = os:system_time(millisecond)
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Check whether the given transaction is blacklisted.
is_tx_blacklisted(TXID) ->
	ets:member(ar_tx_blacklist, TXID)
		orelse ets:member(ar_tx_blacklist_pending_headers, TXID)
			orelse ets:member(ar_tx_blacklist_pending_data, TXID).

%% @doc Check whether the byte with the given global offset is blacklisted.
is_byte_blacklisted(Offset) ->
	case ets:next(ar_tx_blacklist_offsets, Offset - 1) of
		'$end_of_table' ->
			false;
		NextOffset ->
			case ets:lookup(ar_tx_blacklist_offsets, NextOffset) of
				[{NextOffset, Start}] ->
					Offset > Start;
				[] ->
					%% The key should have been just removed, unlucky timing.
					is_byte_blacklisted(Offset)
			end
	end.

%% @doc Notify the server about the removed transaction header.
notify_about_removed_tx(TXID) ->
	gen_server:cast(?MODULE, {removed_tx, TXID}).

%% @doc Notify the server about the removed transaction data.
notify_about_removed_tx_data(TXID) ->
	gen_server:cast(?MODULE, {removed_tx_data, TXID}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	ok = initialize_state(),
	process_flag(trap_exit, true),
	gen_server:cast(?MODULE, refresh_blacklist),
	gen_server:cast(?MODULE, maybe_request_takedown),
	gen_server:cast(?MODULE, maybe_restore_offsets),
	{ok, _} = timer:apply_interval(?STORE_STATE_FREQUENCY_MS, ?MODULE, store_state, []),
	{ok, #ar_tx_blacklist_state{}}.

handle_call(Request, _From, State) ->
	?LOG_ERROR("unhandled call: ~p", [Request]),
	{reply, ok, State}.

handle_cast(refresh_blacklist, State) ->
	refresh_blacklist(),
	{noreply, State};

handle_cast(maybe_request_takedown, State) ->
	#ar_tx_blacklist_state{
		header_takedown_request_timestamp = HTS,
		data_takedown_request_timestamp = DTS
	} = State,
	Now = os:system_time(millisecond),
	State2 =
		case HTS + ?REQUEST_TAKEDOWN_DELAY_MS < Now of
			true ->
				request_header_takedown(State);
			false ->
				State
		end,
	State3 = 
		case DTS + ?REQUEST_TAKEDOWN_DELAY_MS < Now of
			true ->
				request_data_takedown(State2);
			false ->
				State2
		end,
	timer:apply_after(
		?CHECK_PENDING_ITEMS_INTERVAL_MS,
		gen_server,
		cast,
		[self(), maybe_request_takedown]
	),
	{noreply, State3};

handle_cast(maybe_restore_offsets, State) ->
	case ets:first(ar_tx_blacklist_pending_restored_offsets) of
		'$end_of_table' ->
			ok;
		TXID ->
			case ar_data_sync:get_tx_offset(TXID) of
				{ok, {End, Size}} ->
					restore_offsets(End, End - Size),
					ets:delete(ar_tx_blacklist_pending_restored_offsets, TXID);
				{error, not_joined} ->
					ok;
				{error, Reason} ->
					ar:err([
						{event, ar_tx_blacklist_failed_to_fetch_tx_offset},
						{tx, ar_util:encode(TXID)},
						{reason, Reason}
					])
			end
	end,
	timer:apply_after(
		?CHECK_PENDING_ITEMS_INTERVAL_MS,
		gen_server,
		cast,
		[self(), maybe_restore_offsets]
	),
	{noreply, State};

handle_cast({removed_tx, TXID}, State) ->
	case ets:member(ar_tx_blacklist_pending_headers, TXID) of
		false ->
			{noreply, State};
		true ->
			case ets:member(ar_tx_blacklist_pending_data, TXID) of
				false ->
					ets:insert(ar_tx_blacklist, [{TXID}]),
					ets:delete(ar_tx_blacklist_pending_headers, TXID);
				true ->
					ets:delete(ar_tx_blacklist_pending_headers, TXID)
			end,
			{noreply, request_header_takedown(State)}
	end;

handle_cast({removed_tx_data, TXID}, State) ->
	case ets:member(ar_tx_blacklist_pending_data, TXID) of
		false ->
			{noreply, State};
		true ->
			case ets:member(ar_tx_blacklist_pending_headers, TXID) of
				false ->
					ets:insert(ar_tx_blacklist, [{TXID}]),
					ets:delete(ar_tx_blacklist_pending_data, TXID);
				true ->
					ets:delete(ar_tx_blacklist_pending_data, TXID)
			end,
			{noreply, request_data_takedown(State)}
	end;

handle_cast(Msg, State) ->
	?LOG_ERROR("unhandled cast: ~p", [Msg]),
	{noreply, State}.

handle_info(Info, State) ->
	?LOG_ERROR("unhandled info: ~p", [Info]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{event, ar_tx_blacklist_terminate}, {reason, Reason}]),
	store_state(),
	close_dets().

%%%===================================================================
%%% Private functions.
%%%===================================================================

initialize_state() ->
	DataDir = ar_meta_db:get(data_dir),
	Dir = filename:join(DataDir, "ar_tx_blacklist"),
	ok = filelib:ensure_dir(Dir ++ "/"),
	Names = [
		ar_tx_blacklist,
		ar_tx_blacklist_pending_headers,
		ar_tx_blacklist_pending_data,
		ar_tx_blacklist_offsets,
		ar_tx_blacklist_pending_restored_offsets
	],
	lists:foreach(
		fun
			(Name) ->
				{ok, _} = dets:open_file(Name, [{file, filename:join(Dir, Name)}]),
				true = ets:from_dets(Name, Name)
		end,
		Names
	).

refresh_blacklist() ->
	WhitelistFiles = ar_meta_db:get(transaction_whitelist_files),
	case load_from_files(WhitelistFiles) of
		error ->
			timer:apply_after(
				?REFRESH_BLACKLISTS_RETRY_DELAY_MS,
				gen_server,
				cast,
				[self(), refresh_blacklist]
			),
			ok;
		{ok, Whitelist} ->
			WhitelistURLs = ar_meta_db:get(transaction_whitelist_urls),
			case load_from_urls(WhitelistURLs) of
				error ->
					timer:apply_after(
						?REFRESH_BLACKLISTS_RETRY_DELAY_MS,
						gen_server,
						cast,
						[self(), refresh_blacklist]
					),
					ok;
				{ok, Whitelist2} ->
					refresh_blacklist(sets:union(Whitelist, Whitelist2))
			end
	end.

refresh_blacklist(Whitelist) ->
	BlacklistFiles = ar_meta_db:get(transaction_blacklist_files),
	case load_from_files(BlacklistFiles) of
		error ->
			timer:apply_after(
				?REFRESH_BLACKLISTS_RETRY_DELAY_MS,
				gen_server,
				cast,
				[self(), refresh_blacklist]
			),
			ok;
		{ok, Blacklist} ->
			BlacklistURLs = ar_meta_db:get(transaction_blacklist_urls),
			case load_from_urls(BlacklistURLs) of
				error ->
					timer:apply_after(
						?REFRESH_BLACKLISTS_RETRY_DELAY_MS,
						gen_server,
						cast,
						[self(), refresh_blacklist]
					),
					ok;
				{ok, Blacklist2} ->
					refresh_blacklist(Whitelist, sets:union(Blacklist, Blacklist2))
			end
	end.

refresh_blacklist(Whitelist, Blacklist) ->
	Removed =
		sets:fold(
			fun(TXID, Acc) ->
				case not sets:is_element(TXID, Whitelist)
						andalso not ets:member(ar_tx_blacklist, TXID) of
					true ->
						[TXID | Acc];
					false ->
						Acc
				end
			end,
			[],
			Blacklist
		),
	Restored =
		ets:foldl(
			fun({TXID}, Acc) ->
				case sets:is_element(TXID, Whitelist)
						orelse not sets:is_element(TXID, Blacklist) of
					true ->
						[TXID | Acc];
					false ->
						Acc
				end
			end,
			[],
			ar_tx_blacklist
		),
	lists:foreach(
		fun(TXID) ->
			ets:insert(ar_tx_blacklist_pending_headers, [{TXID}]),
			ets:insert(ar_tx_blacklist_pending_data, [{TXID}]),
			ets:delete(ar_tx_blacklist_pending_restored_offsets, TXID)
		end,
		Removed
	),
	lists:foreach(
		fun(TXID) ->
			ets:insert(ar_tx_blacklist_pending_restored_offsets, [{TXID}]),
			ets:delete(ar_tx_blacklist_pending_data, TXID),
			ets:delete(ar_tx_blacklist_pending_headers, TXID),
			ets:delete(ar_tx_blacklist, TXID)
		end,
		Restored
	),
	timer:apply_after(
		?REFRESH_BLACKLISTS_FREQUENCY_MS,
		gen_server,
		cast,
		[self(), refresh_blacklist]
	),
	ok.

load_from_files(Files) ->
	Lists = lists:map(fun load_from_file/1, Files),
	case lists:all(fun(error) -> false; (_) -> true end, Lists) of
		true ->
			{ok, sets:from_list(lists:flatten(Lists))};
		false ->
			error
	end.

load_from_file(File) ->
	try
		{ok, Binary} = file:read_file(File),
		parse_binary(Binary)
	catch Type:Pattern ->
		Warning = [
			{event, failed_to_load_and_parse_file},
			{file, File},
			{exception, {Type, Pattern}}
		],
		?LOG_WARNING(Warning),
		error
	end.

parse_binary(Binary) ->
	lists:filtermap(
		fun(TXID) ->
			case TXID of
				<<>> ->
					false;
				TXIDEncoded ->
					case ar_util:safe_decode(TXIDEncoded) of
						{error, invalid} ->
							false;
						{ok, Decoded} ->
							{true, Decoded}
					end
			end
		end,
		binary:split(Binary, <<"\n">>, [global])
	).

load_from_urls(URLs) ->
	Lists = lists:map(fun load_from_url/1, URLs),
	case lists:all(fun(error) -> false; (_) -> true end, Lists) of
		true ->
			{ok, sets:from_list(lists:flatten(Lists))};
		false ->
			error
	end.

load_from_url(URL) ->
	try
		{ok, {_Scheme, _UserInfo, Host, Port, Path, Query}} =
			http_uri:parse(case is_list(URL) of true -> list_to_binary(URL); _ -> URL end),
		Reply =
			ar_http:req(#{
				method => get,
				peer => {binary_to_list(Host), Port},
				path => binary_to_list(<<Path/binary, Query/binary>>),
				is_peer_request => false,
				timeout => 20000,
				connect_timeout => 1000
			}),
		case Reply of
			{ok, {{<<"200">>, _}, _, Body, _, _}} ->
				parse_binary(Body);
			_ ->
				?LOG_INFO([
					{event, failed_to_download_tx_blacklist},
					{url, URL},
					{reply, Reply}
				]),
				error
		end
	catch Type:Pattern ->
		?LOG_INFO([
			{event, failed_to_load_and_parse_tx_blacklist},
			{url, URL},
			{exception, {Type, Pattern}}
		]),
		error
	end.

request_header_takedown(State) ->
	case ets:first(ar_tx_blacklist_pending_headers) of
		'$end_of_table' ->
			State;
		TXID ->
			ar_header_sync:request_tx_removal(TXID),
			State#ar_tx_blacklist_state{
				header_takedown_request_timestamp = os:system_time(millisecond)
			}
	end.

request_data_takedown(State) ->
	case ets:first(ar_tx_blacklist_pending_data) of
		'$end_of_table' ->
			State;
		TXID ->
			case ar_data_sync:get_tx_offset(TXID) of
				{ok, {End, Size}} ->
					add_offsets(End, End - Size),
					ar_data_sync:request_tx_data_removal(TXID),
					State2 =
						State#ar_tx_blacklist_state{
							data_takedown_request_timestamp = os:system_time(millisecond)
						},
					State2;
				{error, _Reason} ->
					State
			end
	end.

store_state() ->
	Names = [
		ar_tx_blacklist,
		ar_tx_blacklist_pending_headers,
		ar_tx_blacklist_pending_data,
		ar_tx_blacklist_offsets,
		ar_tx_blacklist_pending_restored_offsets
	],
	lists:foreach(
		fun
			(Name) ->
				ets:to_dets(Name, Name)
		end,
		Names
	).

restore_offsets(End, Start) ->
	case ets:next(ar_tx_blacklist_offsets, Start) of
		'$end_of_table' ->
			ok;
		End2 ->
			case ets:lookup(ar_tx_blacklist_offsets, End2) of
				[{_End2, Start2}] when Start2 >= End ->
					ok;
				[{End2, Start2}] ->
					Insert =
						case Start2 < Start of
							true ->
								[{Start, Start2}];
							false ->
								[]
						end,
					Insert2 =
						case End2 > End of
							true ->
								[{End2, End} | Insert];
							false ->
								Insert
						end,
					ets:insert(ar_tx_blacklist_offsets, Insert2),
					case End2 =< End of
						true ->
							ets:delete(ar_tx_blacklist_offsets, End2),
							case End2 < End of
								true ->
									restore_offsets(End, End2);
								false ->
									ok
							end;
						false ->
							ok
					end
			end
	end.

add_offsets(End, Start) ->
	case ets:next(ar_tx_blacklist_offsets, Start) of
		'$end_of_table' ->
			ets:insert(ar_tx_blacklist_offsets, [{End, Start}]);
		End2 ->
			case ets:lookup(ar_tx_blacklist_offsets, End2) of
				[{_End2, Start2}] when Start2 > End ->
					ets:insert(ar_tx_blacklist_offsets, [{End, Start}]);
				[{End2, Start2}] ->
					ets:insert(ar_tx_blacklist_offsets, [{max(End, End2), min(Start, Start2)}]),
					case End2 < End of
						true ->
							ets:delete(ar_tx_blacklist_offsets, End2);
						false ->
							ok
					end
			end
	end.

close_dets() ->
	Names = [
		ar_tx_blacklist,
		ar_tx_blacklist_pending_headers,
		ar_tx_blacklist_pending_data,
		ar_tx_blacklist_offsets,
		ar_tx_blacklist_pending_restored_offsets
	],
	lists:foreach(
		fun
			(Name) ->
				case dets:close(Name) of
					ok ->
						ok;
					{error, Reason} ->
						?LOG_ERROR([
							{event, failed_to_close_dets_table},
							{name, Name},
							{reason, Reason}
						])
				end
		end,
		Names
	).
