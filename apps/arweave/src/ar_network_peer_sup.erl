%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_network_peer_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_peering/2]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     temporary, 1000, Type, [Mod]}).

%% ===================================================================
%% API functions
%% ===================================================================
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------
%% @doc
%% Start new peering handler
%%
%% @spec start_peering(Peer, Options) -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_peering(Peer, Options) ->
    supervisor:start_child(?MODULE, [{Peer, Options}]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
init([]) ->
    {ok, { {simple_one_for_one, 5, 10}, [
        ?CHILD(ar_network_peer, ar_network_peer, worker, [])
    ]} }.

