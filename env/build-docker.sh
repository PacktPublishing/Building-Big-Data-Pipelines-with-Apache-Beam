#!/bin/bash

BASE=$(dirname $0)
ROOT=${BASE}/../

function get_modules() {
  if [ -n "$1" ]; then
    if [ "$1" -eq "$1" ]; then
      echo "-pl :beam-chapter${1} -am"
    else
      echo "-pl :${1} -am"
    fi
  fi
}

set -e
if [[ $(minikube status -f='{{.Host}}') != "Running" ]]; then
  echo "Please run minikube before running this script."
  exit 1
fi

eval $(minikube docker-env)

if ! docker images | grep packt-beam-base > /dev/null; then
  ${BASE}/build-docker-base.sh
fi

MODULES=""
if [[ $# -gt 0 ]]; then
  MODULES=$(get_modules $1)
fi

${ROOT}/mvnw package ${MODULES}

docker build -t packt-beam -f ${ROOT}/docker/Dockerfile ${ROOT}

POD=$(kubectl get pods | grep packt-beam | cut -d' ' -f1)
if [[ ! -z $POD ]]; then
  kubectl delete pod $POD
fi
