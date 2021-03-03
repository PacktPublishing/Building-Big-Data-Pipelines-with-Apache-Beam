#!/bin/bash

for t in $(find . -name target); do
  if [ -d $t ]; then
    cp $(find $t -name *.jar) /usr/local/lib/
  fi
done
