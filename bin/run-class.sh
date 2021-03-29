#!/bin/bash

CLASSPATH=""
for jar in /usr/local/lib/*.jar; do
  if [[ ! -z $CLASSPATH ]]; then
    CLASSPATH="${CLASSPATH}:"
  fi
  CLASSPATH="${CLASSPATH}${jar}"
done

${JAVA_HOME}/bin/java -cp ${CLASSPATH} "$@"
