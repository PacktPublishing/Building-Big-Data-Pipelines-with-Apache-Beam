#!/bin/bash

set -e

BASE=$(dirname $0)

${BASE}/build-docker.sh

kubectl apply -f ${BASE}/manifests/packt_beam.yaml
