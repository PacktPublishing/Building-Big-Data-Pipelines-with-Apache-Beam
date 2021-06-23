#!/bin/bash

set -e

BASE=$(dirname $0)
DOCKER=${BASE}/docker
MANIFESTS=${BASE}/manifests

set -e

if [[ $(minikube status -f='{{.Host}}') != "Running" ]]; then
  echo "Please run minikube before running this script."
  exit 1
fi

eval $(minikube docker-env)
docker build -t flink-packt ${DOCKER}/flink
docker build -t python-harness-packt ${DOCKER}/harness

kubectl apply -f ${MANIFESTS}/flink.yaml
