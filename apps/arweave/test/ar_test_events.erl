-module(ar_test_events).

-include_lib("common_test/include/ct.hrl").
-include_lib("arweave/include/ar.hrl").

-export([
	subscribe_send_cancel/1,
	process_terminated/1
]).

subscribe_send_cancel(Config) ->
	% Check whether all the 'event'-processes are alive.
	% This list should be aligned with the total number
	% of running gen_servers by ar_events_sup
	Processes = [peer],
	true = lists:all(fun(P) -> whereis(ar_events:event_to_process(P)) /= undefined end, Processes),
	EventNetworkStateOnStart = sys:get_state(ar_events:event_to_process(network)),
	ok = ar_events:subscribe(network),
	already_subscribed = ar_events:subscribe(network),
	[ok, already_subscribed] = ar_events:subscribe([peer, network]),
	ok = ar_events:send(network, 12345),
	receive
		{event, network, 12345} ->
			ok
	after 100 ->
		ct:fail("timed out. should have received value")
	end,
	ok = ar_events:cancel(network),
	EventNetworkStateOnStart = sys:get_state(ar_events:event_to_process(network)),
	Config.


process_terminated(Config) ->
	% If a subscriber has been terminated without implicit 'cancel' call
	% it should be cleaned up from the subscription list.
	EventNetworkStateOnStart = sys:get_state(ar_events:event_to_process(network)),
	spawn(fun() -> ar_events:subscribe(network) end),
	timer:sleep(200),
	EventNetworkStateOnStart = sys:get_state(ar_events:event_to_process(network)),
	Config.
