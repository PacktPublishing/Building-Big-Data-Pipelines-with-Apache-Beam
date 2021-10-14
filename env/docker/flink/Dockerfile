FROM apache/beam_java11_sdk:2.33.0 AS harness
FROM apache/flink:1.13.2-scala_2.12-java11

COPY --from=harness /opt/apache /opt/apache

RUN chown 9999:9999 /opt/apache -R



