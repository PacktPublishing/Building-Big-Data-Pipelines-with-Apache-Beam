#!/bin/bash

kubectl exec -it kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh "$@"
