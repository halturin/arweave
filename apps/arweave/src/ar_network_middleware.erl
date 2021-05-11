-module(ar_network_middleware).

-behaviour(cowboy_middleware).

-export([execute/2]).

-include_lib("arweave/include/ar.hrl").

execute(Req, Env) ->
	case cowboy_req:header(<<"x-network">>, Req, <<"arweave.N.1">>) of
		<<?NETWORK_NAME>> ->
			{Peer,_RemotePort} = cowboy_req:peer(Req),
			PortBin= cowboy_req:header(<<"x-p2p-port">>, Req, <<"0">>),
			Port = binary_to_integer(PortBin),
			ar_network:add_peer_candidate(Peer, Port),
			{ok, Req, Env};
		_ ->
			case cowboy_req:method(Req) of
				<<"GET">> ->
					{ok, Req, Env};
				<<"HEAD">> ->
					{ok, Req, Env};
				<<"OPTIONS">> ->
					{ok, Req, Env};
				_ ->
					wrong_network(Req)
			end
	end.

wrong_network(Req) ->
	{stop, cowboy_req:reply(412, #{}, jiffy:encode(#{ error => wrong_network }), Req)}.

