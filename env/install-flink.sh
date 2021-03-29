#!/bin/bash

BASE=$(dirname $0)
MANIFESTS=${BASE}/manifests

set -e

kubectl apply -f ${MANIFESTS}/flink.yaml

