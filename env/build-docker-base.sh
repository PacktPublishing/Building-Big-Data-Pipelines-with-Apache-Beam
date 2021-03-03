#!/bin/bash

BASE=$(dirname $0)
ROOT=${BASE}/../

set -e
if [[ $(minikube status -f='{{.Host}}') != "Running" ]]; then
  echo "Please run minikube before running this script."
  exit 1
fi

eval $(minikube docker-env)

docker build -t packt-beam-base -f ${ROOT}/docker/Dockerfile.base ${ROOT}
