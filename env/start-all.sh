#!/bin/bash

BASE=$(dirname $0)

set -e

MINIKUBE_CPUS=${MINIKUBE_CPUS:-$(cat /proc/cpuinfo | grep processor | wc -l)}
MINIKUBE_MEMORY=${MINIKUBE_MEMORY:-4g}

if [[ $(minikube status -f='{{.Host}}') != "Running" ]]; then
  minikube start --cpus=${MINIKUBE_CPUS} --memory=${MINIKUBE_MEMORY}
fi

${BASE}/install-kafka.sh

${BASE}/install-flink.sh

${BASE}/build-docker.sh

${BASE}/install-packt-beam.sh
