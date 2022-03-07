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
package com.packtpub.beam.chapter2;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Create.Values;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringSerializer;

public class TestKafkaWrite {

  public static void main(String[] args) {
    Values<String> values = Create.of(Arrays.asList(Arrays.copyOfRange(args, 2, args.length)));
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply(values)
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via(e -> KV.of("key", e)))
        .apply(
            KafkaIO.<String, String>write()
                .withValueSerializer(StringSerializer.class)
                .withKeySerializer(StringSerializer.class)
                .withBootstrapServers(args[0])
                .withTopic(args[1]));
    pipeline.run();
  }
}
