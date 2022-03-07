/**
 * Copyright 2021-2022 Packt Publishing Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.packtpub.beam.util;

import com.google.common.collect.ImmutableMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class WritePositionsToKafka {

  private static void usage() {
    System.err.printf(
        "Usage: %s <bootstrap_server> <output_topic>\n",
        WritePositionsToKafka.class.getSimpleName());
    System.exit(1);
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      usage();
    }
    String bootstrapServer = args[0];
    String outputTopic = args[1];
    writeStdinToKafkaWithTimestamps(bootstrapServer, outputTopic);
  }

  private static void writeStdinToKafkaWithTimestamps(String bootstrapServer, String outputTopic)
      throws IOException {
    try (KafkaProducer<String, String> producer =
            new KafkaProducer<>(
                ImmutableMap.<String, Object>builder()
                    .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
                    .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .build());
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {

      reader
          .lines()
          .forEach(line -> outputLineAsPositionWithCreateTimestamp(line, producer, outputTopic));
    }
  }

  private static void outputLineAsPositionWithCreateTimestamp(
      String line, KafkaProducer<String, String> producer, String outputTopic) {

    String[] parts = line.split("\t", 2);
    if (parts.length == 2) {
      String key = parts[0];
      Position.parseFrom(parts[1])
          .ifPresent(
              p ->
                  producer.send(
                      new ProducerRecord<>(
                          outputTopic,
                          null,
                          p.getTimestamp(),
                          key,
                          p.getLatitude() + " " + p.getLongitude())));
    } else {
      System.err.println("Invalid record in input: " + line);
    }
  }
}
