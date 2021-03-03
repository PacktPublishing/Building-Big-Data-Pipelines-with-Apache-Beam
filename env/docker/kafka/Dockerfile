FROM openjdk:11
ENV KAFKA_DATA_DIR=/var/lib/kafka/data \
  KAFKA_HOME=/opt/kafka \
  PATH=$PATH:/opt/kafka/bin

ARG KAFKA_VERSION=2.5.1
ARG KAFKA_DIST=kafka_2.13-${KAFKA_VERSION}
RUN wget -q "https://archive.apache.org/dist/kafka/$KAFKA_VERSION/$KAFKA_DIST.tgz" \
    && tar -xzf "$KAFKA_DIST.tgz" -C /opt \
    && rm -r $KAFKA_DIST.tgz

#COPY log4j.properties /opt/$KAFKA_DIST/config/

RUN set -x \
    && ln -s /opt/$KAFKA_DIST $KAFKA_HOME \
    && mkdir -p $KAFKA_DATA_DIR \
    && useradd kafka \
    && chown kafka:kafka $KAFKA_DATA_DIR \
    && chown kafka:kafka -R $KAFKA_HOME \
    # change `log.dirs` settings
    && sed -i "s/^log.dirs=.\+/log.dirs=\/var\/lib\/kafka\/data\//" /opt/kafka/config/server.properties

USER kafka

