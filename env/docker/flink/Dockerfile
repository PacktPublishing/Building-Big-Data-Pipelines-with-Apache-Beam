FROM apache/beam_java11_sdk:2.32.0 AS harness
FROM apache/flink:1.12.4-scala_2.12-java11

COPY --from=harness /opt/apache /opt/apache

RUN wget https://repository.apache.org/content/repositories/snapshots/org/apache/beam/beam-sdks-java-io-kafka/2.34.0-SNAPSHOT/beam-sdks-java-io-kafka-2.34.0-20211006.070346-27.jar -O /opt/apache/beam/jars/beam-sdks-java-io-kafka.jar
RUN chown 9999:9999 /opt/apache -R



