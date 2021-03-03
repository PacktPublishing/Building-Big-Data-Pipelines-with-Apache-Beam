#!/bin/bash

BASE=$(dirname $0)
DOCKER=${BASE}/docker
MANIFESTS=${BASE}/manifests

set -e

if [[ $(minikube status -f='{{.Host}}') != "Running" ]]; then
  echo "Please run minikube before running this script."
  exit 1
fi

eval $(minikube docker-env)
docker build -t kafka-packt ${DOCKER}/kafka

kubectl apply -f ${MANIFESTS}/zookeeper_micro.yaml
kubectl apply -f ${MANIFESTS}/kafka_micro.yaml
