%% @doc This module is very strongly inspired by OTP's base64 source code.
%% See https://github.com/erlang/otp/blob/93ec8bb2dbba9456395a54551fe9f1e0f86184b1/lib/stdlib/src/base64.erl#L66-L80
-module(ar_base32).

-export([encode/1]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Encode data into a lowercase unpadded RFC 4648 base32 alphabet
encode(Bin) when is_binary(Bin) ->
	encode_binary(Bin, <<>>).

%%%===================================================================
%%% Private functions.
%%%===================================================================

encode_binary(<<>>, A) ->
	A;
encode_binary(<<B1:8>>, A) ->
	<<A/bits, (b32e(B1 bsr 3)):8, (b32e((B1 band 7) bsl 2)):8>>;
encode_binary(<<B1:8, B2:8>>, A) ->
	BB = (B1 bsl 8) bor B2,
	<<A/bits,
		(b32e(BB bsr 11)):8,
		(b32e((BB bsr 6) band 31)):8,
		(b32e((BB bsr 1) band 31)):8,
		(b32e((BB bsl 4) band 31)):8>>;
encode_binary(<<B1:8, B2:8, B3:8>>, A) ->
	BB = (B1 bsl 16) bor (B2 bsl 8) bor B3,
	<<A/bits,
		(b32e(BB bsr 19)):8,
		(b32e((BB bsr 14) band 31)):8,
		(b32e((BB bsr 9) band 31)):8,
		(b32e((BB bsr 4) band 31)):8,
		(b32e((BB bsl 1) band 31)):8>>;
encode_binary(<<B1:8, B2:8, B3:8, B4:8>>, A) ->
	BB = (B1 bsl 24) bor (B2 bsl 16) bor (B3 bsl 8) bor B4,
	<<A/bits,
		(b32e(BB bsr 27)):8,
		(b32e((BB bsr 22) band 31)):8,
		(b32e((BB bsr 17) band 31)):8,
		(b32e((BB bsr 12) band 31)):8,
		(b32e((BB bsr 7) band 31)):8,
		(b32e((BB bsr 2) band 31)):8,
		(b32e((BB bsl 3) band 31)):8>>;
encode_binary(<<B1:8, B2:8, B3:8, B4:8, B5:8, Ls/bits>>, A) ->
	BB = (B1 bsl 32) bor (B2 bsl 24) bor (B3 bsl 16) bor (B4 bsl 8) bor B5,
	encode_binary(
		Ls,
		<<A/bits,
			(b32e(BB bsr 35)):8,
			(b32e((BB bsr 30) band 31)):8,
			(b32e((BB bsr 25) band 31)):8,
			(b32e((BB bsr 20) band 31)):8,
			(b32e((BB bsr 15) band 31)):8,
			(b32e((BB bsr 10) band 31)):8,
			(b32e((BB bsr 5) band 31)):8,
			(b32e(BB band 31)):8>>
	).

-compile({inline, [{b32e, 1}]}).
b32e(X) ->
	element(X+1, {
		$a, $b, $c, $d, $e, $f, $g, $h, $i, $j, $k, $l, $m,
		$n, $o, $p, $q, $r, $s, $t, $u, $v, $w, $x, $y, $z,
		$2, $3, $4, $5, $6, $7, $8, $9
	}).

%%
%% Unit-tests
%%

%% @doc These tests are very strongly inspired by Elixir's Base tests.
%% See https://github.com/elixir-lang/elixir/blob/511a51ba8925daa025d3c2fd410e170c1b651013/lib/elixir/test/elixir/base_test.exs
%%
-include_lib("eunit/include/eunit.hrl").

encode_empty_string_test() ->
	?assertEqual(<<>>, ar_base32:encode(<<>>)).

encode_with_one_pad_test() ->
	?assertEqual(<<"mzxw6yq">>, ar_base32:encode(<<"foob">>)).

encode_with_three_pads_test() ->
	?assertEqual(<<"mzxw6">>, ar_base32:encode(<<"foo">>)).

encode_with_four_pads_test() ->
	?assertEqual(<<"mzxq">>, ar_base32:encode(<<"fo">>)).

encode_with_six_pads_test() ->
	?assertEqual(<<"mzxw6ytboi">>, ar_base32:encode(<<"foobar">>)),
	?assertEqual(<<"my">>, ar_base32:encode(<<"f">>)).

encode_with_no_pads_test() ->
	?assertEqual(<<"mzxw6ytb">>, ar_base32:encode(<<"fooba">>)).
