%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_network_handler_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).
-export([start_link/2]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================
start_link(Handlers) ->
	supervisor:start_link(?MODULE, {Handlers, #{}}).
start_link(Handlers, Args) ->
	supervisor:start_link(?MODULE, {Handlers, Args}).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
init({Handlers, Args}) ->
    Children = lists:map(
        fun(I) ->
            Handler = {ar_network_handler, I},
            {Handler, {ar_network_handler, start_link, [{I, Args}]},
                        permanent, 5000, worker, [ar_network_handler]}
        end, lists:seq(1, Handlers)
    ),
    {ok, { {one_for_one, 5, 10}, Children } }.

