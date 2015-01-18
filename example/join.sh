#!/bin/bash
set -x
A="$@"
B=("${A[@]:1}")
echo $B > $1
