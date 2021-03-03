#!/bin/bash

FLAGS=$([ -t 0 ] && echo "-it" || echo "-i")
kubectl exec ${FLAGS} kafka-0 -- /opt/kafka/bin/kafka-console-producer.sh "$@"
