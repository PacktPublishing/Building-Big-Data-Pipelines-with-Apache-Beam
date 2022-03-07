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
package com.packtpub.beam.chapter5;

import com.packtpub.beam.util.MapToLines;
import com.packtpub.beam.util.Utils;
import com.packtpub.beam.util.WithStringSchema;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import lombok.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.TypedCombineFnDelegate;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

public class SQLMaxWordLength {

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
    PCollection<String> output = computeLongestWord(lines);
    output
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via(e -> KV.of("", e)))
        .apply(
            KafkaIO.<String, String>write()
                .withBootstrapServers(params.getBootstrapServer())
                .withTopic(params.getOutputTopic())
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class));
    pipeline.run().waitUntilFinish();
  }

  static PCollection<String> computeLongestWord(PCollection<String> lines) {
    return lines
        .apply(
            Window.<String>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .withAllowedLateness(Duration.ZERO)
                .accumulatingFiredPanes())
        .apply(WithStringSchema.of("line"))
        .apply(Convert.toRows())
        .apply(
            SqlTransform.query(
                    "WITH tokenized AS (SELECT TOKENIZE(line) AS words FROM PCOLLECTION),\n"
                        + "words AS (SELECT EXPR$0 word FROM UNNEST(SELECT * FROM tokenized))\n"
                        + "SELECT LONGEST_WORD(word) FROM words")
                .registerUdf("TOKENIZE", new TokenizeFn())
                .registerUdaf(
                    "LONGEST_WORD",
                    new TypedCombineFnDelegate<>(
                        Max.of(
                            (Comparator<String> & Serializable)
                                (a, b) -> Integer.compare(a.length(), b.length()))) {}))
        .apply(MapElements.into(TypeDescriptors.strings()).via(r -> r.getString(0)));
  }

  public static class TokenizeFn implements SerializableFunction<String, List<String>> {
    @Override
    public List<String> apply(String input) {
      return Utils.toWords(input);
    }
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
