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

import java.util.Collections;
import java.util.Optional;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Instant;

public class ReadPositionsFromKafka extends PTransform<PBegin, PCollection<KV<String, Position>>> {

  private final String bootstrapServer;
  private final String inputTopic;

  public ReadPositionsFromKafka(String bootstrapServer, String inputTopic) {
    this.bootstrapServer = bootstrapServer;
    this.inputTopic = inputTopic;
  }

  @Override
  public PCollection<KV<String, Position>> expand(PBegin input) {
    return input
        .apply(
            "readInput",
            KafkaIO.<String, String>read()
                .withBootstrapServers(bootstrapServer)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withTopic(inputTopic)
                .withTimestampPolicyFactory(newTimestampPolicy()))
        .apply(
            FlatMapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.strings(), TypeDescriptor.of(Position.class)))
                .via(
                    r -> {
                      String[] kv = r.getKV().getValue().split("\t", 2);
                      if (kv.length > 1) {
                        Optional<Position> parsed = Position.parseFrom(kv[1]);
                        if (parsed.isPresent()) {
                          return Collections.singletonList(KV.of(kv[0], parsed.get()));
                        }
                      }
                      return Collections.emptyList();
                    }))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), new PositionCoder()));
  }

  private static TimestampPolicyFactory<String, String> newTimestampPolicy() {
    return (tp, previousWatermark) ->
        new TimestampPolicy<>() {

          Instant maxTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;

          @Override
          public Instant getTimestampForRecord(
              PartitionContext ctx, KafkaRecord<String, String> record) {
            Instant stamp =
                Position.parseFrom(record.getKV().getValue())
                    .map(p -> Instant.ofEpochMilli(p.getTimestamp()))
                    .orElse(BoundedWindow.TIMESTAMP_MAX_VALUE);
            if (stamp.isAfter(maxTimestamp)) {
              maxTimestamp = stamp;
            }
            return stamp;
          }

          @Override
          public Instant getWatermark(PartitionContext ctx) {
            return maxTimestamp.minus(100);
          }
        };
  }
}
