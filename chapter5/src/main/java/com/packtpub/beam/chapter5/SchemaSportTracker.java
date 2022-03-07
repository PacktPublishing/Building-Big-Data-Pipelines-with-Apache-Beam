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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.packtpub.beam.util.Position;
import com.packtpub.beam.util.PositionCoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class SchemaSportTracker {

  private static Schema rowSchema =
      Schema.of(
          Field.of("workoutId", FieldType.STRING),
          Field.of("latitude", FieldType.DOUBLE),
          Field.of("longitude", FieldType.DOUBLE),
          Field.of("ts", FieldType.INT64));

  private static final Schema METRIC_SCHEMA =
      Schema.of(Field.of("distance", FieldType.DOUBLE), Field.of("duration", FieldType.INT64));

  public static void main(String[] args) {
    Params params = parseArgs(args);
    Pipeline pipeline =
        Pipeline.create(PipelineOptionsFactory.fromArgs(params.getRemainingArgs()).create());
    registerCoders(pipeline);

    PCollection<KV<String, Position>> records =
        pipeline
            .apply(
                KafkaIO.<String, String>read()
                    .withBootstrapServers(params.getBootstrapServer())
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .withTopic(params.getInputTopic())
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
                        }));

    PCollection<Row> metrics = computeTrackerMetrics(records);

    metrics
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via(
                    r -> {
                      double distance = r.getRow(1).getRow(0).getDouble(0);
                      long duration = r.getRow(1).getRow(0).getInt64(1);
                      return KV.of(
                          "",
                          r.getRow(0).getString(0)
                              + "\t"
                              + distance
                              + "\t"
                              + (duration > 0 ? (duration * 1000 / (60.0 * distance)) : 0));
                    }))
        .apply(
            KafkaIO.<String, String>write()
                .withBootstrapServers(params.getBootstrapServer())
                .withTopic(params.getOutputTopic())
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class));
    pipeline.run().waitUntilFinish();
  }

  @VisibleForTesting
  static PCollection<Row> computeTrackerMetrics(PCollection<KV<String, Position>> records) {

    return records
        .apply(
            "globalWindow",
            Window.<KV<String, Position>>into(new GlobalWindows())
                .withTimestampCombiner(TimestampCombiner.LATEST)
                .withAllowedLateness(Duration.standardHours(1))
                .triggering(
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.standardSeconds(10))))
                .accumulatingFiredPanes())
        .setSchema(
            rowSchema,
            TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(Position.class)),
            kv ->
                Row.withSchema(rowSchema)
                    .attachValues(
                        kv.getKey(),
                        kv.getValue().getLatitude(),
                        kv.getValue().getLongitude(),
                        kv.getValue().getTimestamp()),
            r -> KV.of(r.getString(0), new Position(r.getDouble(1), r.getDouble(2), r.getInt64(3))))
        .apply(
            Group.<KV<String, Position>>byFieldIds(0)
                .aggregateFields(
                    Arrays.asList("latitude", "longitude", "ts"),
                    new MetricUdaf(),
                    Field.of("metric", FieldType.row(METRIC_SCHEMA))));
  }

  private static Row computeRawMetrics(Iterable<Row> positions) {

    List<Position> sortedPositions =
        Streams.stream(positions)
            .map(SchemaSportTracker::toPosition)
            .filter(Objects::nonNull)
            .sorted(Comparator.comparingLong(Position::getTimestamp))
            .collect(Collectors.toList());

    @Nullable Position first = null;
    double distance = 0;
    long startTs = Long.MIN_VALUE;
    for (Position p : sortedPositions) {
      if (first != null) {
        distance += first.distance(p);
      } else {
        startTs = p.getTimestamp();
      }
      first = p;
    }
    long duration = first != null ? first.getTimestamp() - startTs : 0L;
    return Row.withSchema(METRIC_SCHEMA).attachValues(distance, duration);
  }

  @Nullable
  private static Position toPosition(Row r) {
    return new Position(r.getDouble(0), r.getDouble(1), r.getInt64(2));
  }

  private static TimestampPolicyFactory<String, String> newTimestampPolicy() {
    return (tp, previousWatermark) ->
        new CustomTimestampPolicyWithLimitedDelay<>(
            record ->
                Position.parseFrom(record.getKV().getValue())
                    .map(p -> Instant.ofEpochMilli(p.getTimestamp()))
                    .orElse(BoundedWindow.TIMESTAMP_MIN_VALUE),
            Duration.standardSeconds(1),
            previousWatermark);
  }

  static Params parseArgs(String[] args) {
    if (args.length < 3) {
      throw new IllegalArgumentException(
          "Expected at least 3 arguments: <bootstrapServer> <inputTopic> <outputTopic>");
    }
    return new Params(args[0], args[1], args[2], Arrays.copyOfRange(args, 3, args.length));
  }

  @VisibleForTesting
  static void registerCoders(Pipeline pipeline) {
    pipeline.getCoderRegistry().registerCoderForClass(Position.class, new PositionCoder());
  }

  @Value
  static class Params {
    String bootstrapServer;
    String inputTopic;
    String outputTopic;
    String[] remainingArgs;
  }

  public static class MetricUdaf extends CombineFn<Row, List<Row>, Row> {

    @Override
    public List<Row> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<Row> addInput(List<Row> mutableAccumulator, Row input) {
      mutableAccumulator.add(input);
      return mutableAccumulator;
    }

    @Override
    public List<Row> mergeAccumulators(Iterable<List<Row>> accumulators) {
      List<Row> res = new ArrayList<>();
      accumulators.forEach(res::addAll);
      return res;
    }

    @Override
    public Row extractOutput(List<Row> accumulator) {
      return computeRawMetrics(accumulator);
    }
  }
}
