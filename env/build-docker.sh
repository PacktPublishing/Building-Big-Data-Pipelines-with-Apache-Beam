#!/bin/bash

BASE=$(dirname $0)
ROOT=${BASE}/../

set -e
if [[ $(minikube status -f='{{.Host}}') != "Running" ]]; then
  echo "Please run minikube before running this script."
  exit 1
fi

eval $(minikube docker-env)

if ! docker images | grep packt-beam-base > /dev/null; then
  ${BASE}/build-docker-base.sh
fi

${ROOT}/mvnw package

docker build -t packt-beam -f ${ROOT}/docker/Dockerfile ${ROOT}

POD=$(kubectl get pods | grep packt-beam | cut -d' ' -f1)
if [[ ! -z $POD ]]; then
  kubectl delete pod $POD
fi
