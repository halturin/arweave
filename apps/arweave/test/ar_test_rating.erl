-module(ar_test_rating).

-include_lib("common_test/include/ct.hrl").
-include_lib("arweave/include/ar.hrl").

-include_lib("arweave/include/ar_rating.hrl").

-export([
	peer_join_leave_rejoin/1,
	check_rate_and_triggers/1,
	check_get_top_n_get_banned/1

]).

peer_join_leave_rejoin(_Config) ->
	{state, Joined, _Changed, Rates, _Triggers, RatingDB} = sys:get_state(ar_rating),
	case Joined of
		false ->
			true = check_network_join(),
			{state, true, _Changed, Rates, _Triggers, RatingDB} = sys:get_state(ar_rating);
		_ ->
			ok
	end,
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
	{state, true, _Changed1, _Rates1, _Triggers1, RatingDB1} = sys:get_state(ar_rating),
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

check_rate_and_triggers(_Config) ->
	{state, Joined, _Changed, Rates, _Triggers, RatingDB} = sys:get_state(ar_rating),
	case Joined of
		false ->
			true = check_network_join(),
			{state, true, _Changed, Rates, _Triggers, RatingDB} = sys:get_state(ar_rating);
		_ ->
			ok
	end,
	PeerA = peerA,
	% make sure if this peer was joined. otherwise all the events will be ignored
	ok = ar_events:send(peer, {joined, PeerA}),
	% generate events
	EventPeer = #event_peer{
		peer = PeerA,
		time = 0
	},
	RateRequestTX = maps:get({request, tx}, Rates, 0),
	RateRequestBlock = maps:get({request, block}, Rates, 0),
	RateRequestChunk = maps:get({request, chunk}, Rates, 0),
	ct:print("~p - ~p - ~p", [RateRequestTX, RateRequestBlock, RateRequestChunk]),
	ok = ar_events:send(peer, {request, tx, EventPeer}),
	ok = ar_events:send(peer, {request, block, EventPeer}),
	ok = ar_events:send(peer, {request, chunk, EventPeer}),
	timer:sleep(100),
	gen_server:cast(ar_rating, compute_rating),
	timer:sleep(100),
	PeerA_rating = ar_rating:rate(RateRequestTX, 0)
				+ ar_rating:rate(RateRequestBlock, 0)
				+ ar_rating:rate(RateRequestChunk, 0),
	case ets:lookup(ar_rating, {peer, PeerA}) of
		[{_, Rating}] ->
			% should be the same
			PeerA_rating = Rating#rating.r;
		_ ->
			ct:fail("got wrong rating from ets")
	end,
	ok.

check_get_top_n_get_banned(_Config) ->
	{state, Joined, _Changed, Rates, _Triggers, RatingDB} = sys:get_state(ar_rating),
	case Joined of
		false ->
			true = check_network_join(),
			{state, true, _Changed, Rates, _Triggers, RatingDB} = sys:get_state(ar_rating);
		_ ->
			ok
	end,

	RateSamples = [{peerA,100,false}, {peerB,1,false}, {peerC,1,true},
				   {peerD,200,false}, {peerE,300,true}, {peerF,250, false},
				   {peerG,10, false}, {peerH,15, false}, {peerI,1000, true},
				   {peerK,999, false}, {peerL,2,false}, {peerM,8,true},
				   {peerN, 99,false}],
	lists:map(fun({Peer, R, Banned}) ->
				Ban = case Banned of
						true ->
							  os:system_time(second) + 1000;
						_ ->
							  0
					end,
				Rating = #rating{r = R, ban = Ban},
				RatingBinary = term_to_binary(Rating),
				PeerBinary = term_to_binary(Peer),
				ar_kv:put(RatingDB, PeerBinary, RatingBinary),
				ets:insert(ar_rating, {{peer, Peer}, Rating})
			end, RateSamples),

	% get_top
	[{peerK,999}] = ar_rating:get_top(1),

	[{peerK,999}, {peerF,250}, {peerD,200}, {peerA,100},
	{peerN,99}, {peerH,15}, {peerG,10}] = ar_rating:get_top(7),

	[{peerK,999}, {peerF,250}, {peerD,200}, {peerA,100},
	{peerN,99}, {peerH,15}, {peerG,10}, {peerL,2}, {peerB,1}] = ar_rating:get_top(100),

	% get_top_joined
	%[{peerK,999}] = ar_rating:get_top_joined(1),

	%[{peerK,999}, {peerF,250}, {peerD,200}, {peerA,100},
	%{peerN,99}, {peerH,15}, {peerG,10}] = ar_rating:get_top_joined(7),

	%[{peerK,999}, {peerF,250}, {peerD,200}, {peerA,100},
	%{peerN,99}, {peerH,15}, {peerG,10}, {peerL,2}, {peerB,1}] = ar_rating:get_top_joined(100),

	% get_banned
	[{peerM,8},{peerI,1000},{peerE,300},{peerC,1}] = ar_rating:get_banned(),

	% get peer info {Rating, Host, Port}.
	{100, undefined, 1984} = ar_rating:get(peerA),
	ok.


%% Private functions

check_network_join() ->
	{state, Joined, _Changed, _Rates, _Triggers, _RatingDB} = sys:get_state(ar_rating),
	check_network_join(Joined).
check_network_join(false) ->
	% emulate event that we joined to the arweave network. otherwise everything
	% will be ignored
	ar_events:send(network, joined),
	% should be enough
	timer:sleep(100),
	{state, Joined, _Changed, _Rates, _Triggers, _RatingDB} = sys:get_state(ar_rating),
	Joined;
check_network_join(true) ->
	true.

