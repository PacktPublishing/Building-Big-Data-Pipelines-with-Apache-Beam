#!/bin/bash

IFS=$'\n'
SLEEP=${1:-0.1}
for line in $(cat - | grep -v "^ \*$"); do
  echo $line
  sleep $SLEEP
done
