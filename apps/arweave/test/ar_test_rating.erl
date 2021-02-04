-module(ar_test_rating).

-include_lib("common_test/include/ct.hrl").
-include_lib("arweave/include/ar.hrl").

-include_lib("arweave/include/ar_rating.hrl").

-export([
	join/1,
	rejoin/1,
	check_requests_MA/1,
	check_requests_bonuses/1,
	check_requests_penalties/1,
	check_ban_and_limit/1,
	check_peer_lifespan/1
]).

join(_Config) ->
	check_join(),
	{state, _Joined, _Peers, _Options, _Resp, _Rates, RatingDB} = sys:get_state(ar_rating),
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
	case ar_kv:get(RatingDB, term_to_binary(Peer)) of
		not_found ->
			ct:fail("Peer is not found");
		{ok, _} ->
			ok;
		{error, E2} ->
			ct:fail("Something went wrong ~p", [E2]);
		WTF2 ->
			ct:fail("WTF ~p", [WTF2])
	end,

	{state, _Joined1, Peers, _Options1, _Resp1, _Rates, _DB1} = sys:get_state(ar_rating),
	case maps:get(Peer, Peers, unknown) of
		unknown ->
			ct:fail("Peer is not found in the rating' state");
		_ ->
			ok
	end,
	ok.


rejoin(_Config) ->
	% restart ar_rating process to clear the state
	exit(whereis(ar_rating), kill),
	% wait a bit.
	timer:sleep(100),
	check_join(),
	Peer = peer1,
	{state, _Joined, Peers, _Options, _Resp, RatingDB} = sys:get_state(ar_rating),
	case ar_kv:get(RatingDB, term_to_binary(Peer)) of
		not_found ->
			ct:fail("Peer is not found");
		{ok, _} ->
			ok;
		{error, E2} ->
			ct:fail("Something went wrong ~p", [E2]);
		WTF2 ->
			ct:fail("WTF ~p", [WTF2])
	end,
	case maps:get(Peer, Peers, unknown) of
		unknown ->
			ok;
		_ ->
			ct:fail("is ar_rating restarted? state hasn't cleared up")
	end,
	ok = ar_events:send(peer, {joined, Peer}),
	% just to make sure if this message processed
	timer:sleep(200),
	{state, _Joined1, Peers1, _Options1, _Resp1, _DB1} = sys:get_state(ar_rating),
	case maps:get(Peer, Peers1, unknown) of
		unknown ->
			ct:fail("Peer is not found in the rating' state");
		_ ->
			ok
	end,
	ok.

check_requests_MA(_Config) ->
	ok.
check_requests_bonuses(_Config) ->
	ok.
check_requests_penalties(_Config) ->
	ok.
check_ban_and_limit(_Config) ->
	ok.
check_peer_lifespan(_Config) ->
	ok.

%% Private functions

check_join() ->
	{state, Joined, _Peers, _Options, _Resp, _RatingDB} = sys:get_state(ar_rating),
	check_join(Joined).
check_join(false) ->
	% emulate event that we joined to the arweave network. otherwise everything
	% will be ignored
	ar_events:send(network, joined),
	% should be enough
	timer:sleep(100);
check_join(true) ->
	ok.

