%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-ifndef(AR_RATING_HRL).
-define(AR_RATING_HRL, true).

-record(event_peer, {
	peer = unknown,
	request = any :: atom(),
	time = 0 :: non_neg_integer() % response time or request timestamp
}).

%% @doc The record contains parameters used to compute the Wildfire score
%% for a peer, including the Wildfire score itself.
-record(rating, {
	%% The Wildfire score.
	r = 0,
	%% The timestamp for when we started peering.
	since = os:system_time(second),
	%% rate group keeps the accumulated value of rated action which
	%% is defined in 'rates' map.
	%% Key:
	%%   is the tuple with two values
	%%   {Act, Positive}
	%% 	Act - is the first value of a key tuple in the rates map
	%% 	Positive - true or false.
	%% Value:
	%%   tuple with two values
	%%   {N, History}
	%%   N - accumulated value
	%%   History - list of timestamps
	%% example: {response, false} => {-123, [1613147757, 1613147333]}
	rate_group = #{},
	%% The timestamp of the last record update.
	last_update = os:system_time(second),
	%% The timestamp for when to unban the banned peer.
	ban = 0,
	%% The IP address or the hostname the peer uses (a part of the identifier).
	host,
	%% The TCP port the peer uses (a part of the identifier).
	port = 1984
}).

-endif.
