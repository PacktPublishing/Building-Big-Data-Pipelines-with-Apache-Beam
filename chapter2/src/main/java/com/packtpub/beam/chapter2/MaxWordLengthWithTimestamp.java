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

import static com.packtpub.beam.chapter2.MaxWordLength.parseArgs;

import com.packtpub.beam.chapter2.MaxWordLength.Params;
import com.packtpub.beam.util.MapToLines;
import com.packtpub.beam.util.Tokenize;
import java.io.Serializable;
import java.util.Comparator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

public class MaxWordLengthWithTimestamp {

  public static void main(String[] args) {
    runWithArgsAndCombiner(args, TimestampCombiner.END_OF_WINDOW);
  }

  static void runWithArgsAndCombiner(String[] args, TimestampCombiner combiner) {
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
    PCollection<KV<String, String>> output = computeLongestWordWithTimestamp(lines, combiner);
    output.apply(
        KafkaIO.<String, String>write()
            .withBootstrapServers(params.getBootstrapServer())
            .withTopic(params.getOutputTopic())
            .withKeySerializer(StringSerializer.class)
            .withValueSerializer(StringSerializer.class));
    pipeline.run().waitUntilFinish();
  }

  static PCollection<KV<String, String>> computeLongestWordWithTimestamp(
      PCollection<String> lines) {

    return computeLongestWordWithTimestamp(lines, TimestampCombiner.LATEST);
  }

  static PCollection<KV<String, String>> computeLongestWordWithTimestamp(
      PCollection<String> lines, TimestampCombiner combiner) {
    return lines
        .apply(Tokenize.of())
        .apply(
            Window.<String>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .withAllowedLateness(Duration.ZERO)
                .withTimestampCombiner(combiner)
                .accumulatingFiredPanes())
        .apply(
            Max.globally(
                (Comparator<String> & Serializable) (a, b) -> Long.compare(a.length(), b.length())))
        .apply(Reify.timestamps())
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via(tv -> KV.of(tv.getTimestamp().toString(), tv.getValue())));
  }
}
