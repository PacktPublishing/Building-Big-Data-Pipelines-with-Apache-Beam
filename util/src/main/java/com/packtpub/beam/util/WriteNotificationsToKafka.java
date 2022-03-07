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

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringSerializer;

public class WriteNotificationsToKafka extends PTransform<PCollection<KV<String, Boolean>>, PDone> {

  private final String boostrapServer;
  private final String outputTopic;

  public WriteNotificationsToKafka(String bootstrapServer, String outputTopic) {
    this.boostrapServer = bootstrapServer;
    this.outputTopic = outputTopic;
  }

  @Override
  public PDone expand(PCollection<KV<String, Boolean>> input) {
    return input
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via(kv -> KV.of("", formatMessage(kv))))
        .apply(
            "writeOutput",
            KafkaIO.<String, String>write()
                .withBootstrapServers(boostrapServer)
                .withTopic(outputTopic)
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class));
  }

  private String formatMessage(KV<String, Boolean> message) {
    String user = message.getKey().split(":")[0];
    if (Boolean.TRUE.equals(message.getValue())) {
      return String.format("Great job user %s! You are gaining speed!", user);
    }
    return String.format("Looks like you are slowing down user %s. Go! Go! Go!", user);
  }
}
