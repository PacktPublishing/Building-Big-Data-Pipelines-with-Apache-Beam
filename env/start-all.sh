#!/bin/bash

BASE=$(dirname $0)

set -e

MINIKUBE_CPUS=${MINIKUBE_CPUS:-$(cat /proc/cpuinfo | grep processor | wc -l)}
MINIKUBE_MEMORY=${MINIKUBE_MEMORY:-4g}
MINIKUBE_DISK=${MINIKUBE_DISK:-10g}

sudo sysctl net/netfilter/nf_conntrack_max=524288

if [[ $(minikube status -f='{{.Host}}') != "Running" ]]; then
  minikube start --cpus=${MINIKUBE_CPUS} --memory=${MINIKUBE_MEMORY} --disk-size=${MINIKUBE_DISK}
fi

${BASE}/install-kafka.sh

${BASE}/install-flink.sh

${BASE}/build-docker.sh

${BASE}/install-packt-beam.sh
