#!/bin/bash

BASE=$(dirname $0)

while read line; do
  readarray -d ';' -t arr <<< "$line"
  d=$(echo $line | cut -d';' -f1)
  value=$(echo $line | cut -d';' -f2-)
  if [[ -n $d ]] && [[ -n $value ]]; then
    date=$(date --iso-8601=seconds --date "${d}")
    if [[ -n ${date} ]]; then
      echo "${date};${value}"
    else
      echo "Unparsable date: ${d}" > /dev/stderr
    fi
  else
    echo "Missing semicolon in ${line}" > /dev/stderr
  fi
done

