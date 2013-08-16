#!/bin/sh
erl  -Q 10000000 +P 500000  +A 4 +K true -pa ebin -pa deps/*/ebin  -name prolog@localhost.localdomain -config simple
