-module(ar_test_events).

-include_lib("common_test/include/ct.hrl").
-include_lib("arweave/include/ar.hrl").

-export([
	subscribe/1,
	cancel/1,
	send/1,
	process_terminated/1
]).

subscribe(_Config) ->
	ok.

cancel(_Config) ->
	ok.

send(_Config) ->
	ok.

process_terminated(_Config) ->
	ok.
