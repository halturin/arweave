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


-endif.
