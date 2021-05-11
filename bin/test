#!/usr/bin/env bash

echo -e "\033[0;32m===> Running Unit-tests...\033[0m"
./rebar3 eunit --module=ar_network

echo -e "\033[0;32m===> Running Common-tests...\033[0m"
./rebar3 ct --verbose --sname=master --suite=apps/arweave/test/ar_SUITE --group=nodeNetwork


