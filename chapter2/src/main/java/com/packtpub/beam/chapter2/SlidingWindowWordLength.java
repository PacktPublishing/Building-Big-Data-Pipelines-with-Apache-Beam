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

import com.packtpub.beam.util.MapToLines;
import com.packtpub.beam.util.Tokenize;
import java.util.Arrays;
import lombok.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

public class SlidingWindowWordLength {

  public static void main(String[] args) {
    Params params = parseArgs(args);
    Pipeline pipeline =
        Pipeline.create(PipelineOptionsFactory.fromArgs(params.getRemainingArgs()).create());
    PCollection<String> words =
        pipeline
            .apply(
                KafkaIO.<String, String>read()
                    .withBootstrapServers(params.getBootstrapServer())
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .withTopic(params.getInputTopic()))
            .apply(MapToLines.of())
            .apply(Tokenize.of());

    calculateAverageWordLength(words)
        .apply(WithKeys.of(""))
        .apply(
            KafkaIO.<String, Double>write()
                .withBootstrapServers(params.getBootstrapServer())
                .withTopic(params.getOutputTopic())
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(DoubleSerializer.class));

    pipeline.run().waitUntilFinish();
  }

  static PCollection<Double> calculateAverageWordLength(PCollection<String> words) {
    return words
        .apply(
            Window.into(
                SlidingWindows.of(Duration.standardSeconds(10)).every(Duration.standardSeconds(2))))
        .apply(Combine.globally(new AverageWordLength.AverageFn()).withoutDefaults());
  }

  static Params parseArgs(String[] args) {
    if (args.length < 3) {
      throw new IllegalArgumentException(
          "Expected at least 3 arguments: <bootstrapServer> <inputTopic> <outputTopic>");
    }
    return new Params(args[0], args[1], args[2], Arrays.copyOfRange(args, 3, args.length));
  }

  @Value
  static class Params {
    String bootstrapServer;
    String inputTopic;
    String outputTopic;
    String[] remainingArgs;
  }
}
