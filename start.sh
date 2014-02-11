#!/bin/sh
LC_CTYPE=en_US.UTF-8  erl  -Q 10000000 +P 500000  +A 4 +K true -s eprolog_app  -pa ebin -pa deps/*/ebin  -name prolog@localhost.localdomain -config simple -setcookie my_123


