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

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import com.packtpub.beam.util.MapToLines;
import com.packtpub.beam.util.Tokenize;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import lombok.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

public class TopKWords {

  public static void main(String[] args) {
    Params params = parseArgs(args);
    PipelineOptions options = PipelineOptionsFactory.fromArgs(params.getRemainingArgs()).create();
    Pipeline pipeline = Pipeline.create(options);
    PCollection<String> lines =
        pipeline
            .apply(
                KafkaIO.<String, String>read()
                    .withBootstrapServers(params.getBootstrapServer())
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .withTopic(params.getInputTopic()))
            .apply(MapToLines.of());
    PCollection<KV<String, Long>> output =
        countWordsInFixedWindows(lines, params.getWindowLength(), params.getK());
    output.apply(
        KafkaIO.<String, Long>write()
            .withBootstrapServers(params.getBootstrapServer())
            .withTopic(params.getOutputTopic())
            .withKeySerializer(StringSerializer.class)
            .withValueSerializer(LongSerializer.class));
    pipeline.run().waitUntilFinish();
  }

  @VisibleForTesting
  static PCollection<KV<String, Long>> countWordsInFixedWindows(
      PCollection<String> lines, Duration windowLength, int k) {

    return lines
        .apply(Window.into(FixedWindows.of(windowLength)))
        .apply(Tokenize.of())
        .apply(Count.perElement())
        .apply(
            Top.of(
                    k,
                    (Comparator<KV<String, Long>> & Serializable)
                        (a, b) -> Long.compare(a.getValue(), b.getValue()))
                .withoutDefaults())
        .apply(Flatten.iterables());
  }

  @VisibleForTesting
  static Params parseArgs(String[] args) {
    if (args.length < 5) {
      throw new IllegalArgumentException(
          "Expected at least 5 arguments: <windowLength> <bootstrapServer> <inputTopic> <outputTopic> <k>");
    }
    return new Params(
        Duration.standardSeconds(Integer.parseInt(args[0])),
        args[1],
        args[2],
        args[3],
        Integer.parseInt(args[4]),
        Arrays.copyOfRange(args, 5, args.length));
  }

  @Value
  @VisibleForTesting
  static class Params {
    Duration windowLength;
    String bootstrapServer;
    String inputTopic;
    String outputTopic;
    int k;
    String[] remainingArgs;
  }
}
