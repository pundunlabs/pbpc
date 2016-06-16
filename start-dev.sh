#!/bin/sh
export PRODDIR=`pwd`

name=${1:-pbpc}

exec erl \
    -pa ebin deps/*/ebin \
    -boot start_sasl \
    -sname $name \
    -s ssl start \
    -s gb_log_sup start_link \
    -s pbpc_app start
