-module(ar_test_rating).

-include_lib("common_test/include/ct.hrl").
-include_lib("arweave/include/ar.hrl").

-include_lib("arweave/include/ar_rating.hrl").

-export([
	peer_join_leave_rejoin/1,
	check_triggers/1

]).

peer_join_leave_rejoin(_Config) ->
	{state, false, _Options, _Changed, _Rates, _Triggers, RatingDB} = sys:get_state(ar_rating),
	true = check_network_join(),
	{state, true, _Options, _Changed, _Rates, _Triggers, RatingDB} = sys:get_state(ar_rating),
	Peer = peer1,
	case ar_kv:get(RatingDB, term_to_binary(Peer)) of
		not_found ->
			ok;
		{ok, _} ->
			ct:fail("Peer is already exist");
		{error, E1} ->
			ct:fail("Something went wrong ~p", [E1]);
		WTF1 ->
			ct:fail("WTF ~p", [WTF1])
	end,
	ok = ar_events:send(peer, {joined, Peer}),
	% just to make sure if this message processed
	timer:sleep(200),
	Rating = case ar_kv:get(RatingDB, term_to_binary(Peer)) of
		not_found ->
			ct:fail("Peer is not found");
		{ok, _} ->
			ok;
		{error, E2} ->
			ct:fail("Something went wrong ~p", [E2]);
		WTF2 ->
			ct:fail("WTF ~p", [WTF2])
	end,
	case ets:lookup(ar_rating, {peer, Peer}) of
		[] ->
			ct:fail("Peer is not found");
		[{_, _}] ->
			ok;
		WTF3 ->
			ct:fail("expecting {Rating, History}. got ~p", [WTF3])
	end,


	% restart ar_rating process to clear the state
	gen_server:stop(ar_rating),
	% wait a bit.
	timer:sleep(100),
	% ets table should belongs to the supervisor so this restart shouldnt
	% affect it
	case ets:lookup(ar_rating, {peer, Peer}) of
		[] ->
			ct:fail("Peer is not found");
		[{_, _}] ->
			ok;
		WTF4 ->
			ct:fail("expecting {Rating, History}. got ~p", [WTF4])
	end,
	% restarting process shouldn't affect 'joined' state.
	{state, true, _Options1, _Changed1, _Rates1, _Triggers1, RatingDB1} = sys:get_state(ar_rating),
	% test peer leaving
	ok = ar_events:send(peer, {left, Peer}),
	timer:sleep(100),
	case ets:lookup(ar_rating, {peer, Peer}) of
		[] ->
			ok;
		[{_, _}] ->
			ct:fail("Peer is still there");
		WTF5 ->
			ct:fail("expecting empty list. got ~p", [WTF5])
	end,
	ok = ar_events:send(peer, {joined, Peer}),
	timer:sleep(100),
	% Rating value should be the same as it was before the leaving
	% (this value was assigned earlier. see code above)
	Rating = case ar_kv:get(RatingDB1, term_to_binary(Peer)) of
		not_found ->
			ct:fail("Peer is not found");
		{ok, _} ->
			ok;
		{error, E3} ->
			ct:fail("Something went wrong ~p", [E3]);
		WTF6 ->
			ct:fail("WTF ~p", [WTF6])
	end.

check_triggers(_Config) ->
	ok.


%% Private functions

check_network_join() ->
	{state, Joined, _Options, _Changed, _Rates, _Triggers, _RatingDB} = sys:get_state(ar_rating),
	check_network_join(Joined).
check_network_join(false) ->
	% emulate event that we joined to the arweave network. otherwise everything
	% will be ignored
	ar_events:send(network, joined),
	% should be enough
	timer:sleep(100),
	{state, Joined, _Options, _Changed, _Rates, _Triggers, _RatingDB} = sys:get_state(ar_rating),
	Joined;
check_network_join(true) ->
	true.

