#!/usr/bin/env bash

echo -e "\033[0;32m===> Running Common-tests shell...\033[0m"
ct_run -sname master -pa `./rebar3 path` -shell

