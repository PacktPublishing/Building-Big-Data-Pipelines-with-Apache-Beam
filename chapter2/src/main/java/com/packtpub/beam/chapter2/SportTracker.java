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

import static com.packtpub.beam.util.Position.EARTH_DIAMETER;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.packtpub.beam.util.Position;
import com.packtpub.beam.util.PositionCoder;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class SportTracker {

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

    PCollection<KV<String, Metric>> metrics = computeTrackerMetrics(records);

    metrics
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via(
                    kv ->
                        KV.of(
                            "",
                            kv.getKey()
                                + "\t"
                                + kv.getValue().getLength()
                                + "\t"
                                + (kv.getValue().getTime() > 0
                                    ? (kv.getValue().getTime()
                                        * 1000
                                        / (60.0 * kv.getValue().getLength()))
                                    : 0))))
        .apply(
            KafkaIO.<String, String>write()
                .withBootstrapServers(params.getBootstrapServer())
                .withTopic(params.getOutputTopic())
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class));
    pipeline.run().waitUntilFinish();
  }

  @VisibleForTesting
  static PCollection<KV<String, Metric>> computeTrackerMetrics(
      PCollection<KV<String, Position>> records) {

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
        .apply("groupByWorkoutId", GroupByKey.create())
        .apply(
            FlatMapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(Metric.class)))
                .via(SportTracker::computeRawMetrics));
  }

  private static Iterable<KV<String, Metric>> computeRawMetrics(
      KV<String, Iterable<Position>> stringIterableKV) {

    List<Position> sortedPositions =
        Streams.stream(stringIterableKV.getValue())
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
    long duration = first.getTimestamp() - startTs;
    return Collections.singletonList(
        KV.of(stringIterableKV.getKey(), new Metric(distance, duration)));
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
    pipeline.getCoderRegistry().registerCoderForClass(Metric.class, new MetricCoder());
  }

  @Value
  static class Metric {
    double length;
    long time;
  }

  static class MetricCoder extends CustomCoder<Metric> {

    @Override
    public void encode(Metric value, OutputStream outStream) throws CoderException, IOException {
      DataOutputStream dos = new DataOutputStream(outStream);
      dos.writeDouble(value.getLength());
      VarInt.encode(value.getTime(), dos);
    }

    @Override
    public Metric decode(InputStream inStream) throws CoderException, IOException {
      DataInputStream dis = new DataInputStream(inStream);
      return new Metric(dis.readDouble(), VarInt.decodeLong(dis));
    }
  }

  @Value
  static class Params {
    String bootstrapServer;
    String inputTopic;
    String outputTopic;
    String[] remainingArgs;
  }

  /**
   * Generator of data. The generator generates a random walk from given starting position. This is
   * internal tool and should not be needed.
   */
  public static class Generator {
    public static void main(String[] args) {
      if (args.length < 1) {
        throw new IllegalArgumentException("Please pass number of tracks to generate");
      }
      int numTracks = Integer.parseInt(args[0]);
      long timeStart = args.length > 1 ? Long.parseLong(args[1]) : System.currentTimeMillis();
      Random random = new Random();
      for (int t = 1; t <= numTracks; t++) {
        int numPointsPerTrack = random.nextInt(150) + 10;
        String trackId = "track" + t;
        generateTrack(numPointsPerTrack, timeStart, random)
            .forEach(
                p ->
                    System.out.println(
                        trackId
                            + "\t"
                            + p.getLatitude()
                            + "\t"
                            + p.getLongitude()
                            + "\t"
                            + p.getTimestamp()));
      }
    }

    private static List<Position> generateTrack(
        int numPointsPerTrack, long timeStart, Random random) {

      List<Position> ret = new ArrayList<>();
      double averageMinutesPerKm = random.nextDouble() * 5 + 3;
      double averageMetersPerSecond = 1000. / (60 * averageMinutesPerKm);
      double meterToAngle = 180 / (EARTH_DIAMETER * Math.PI);

      for (int i = 0; i < numPointsPerTrack; i++) {
        if (i == 0) {
          ret.add(
              new Position(
                  random.nextDouble() * 180 - 90, random.nextDouble() * 180 - 90, timeStart));
        } else {
          Position lastPos = ret.get(i - 1);
          long timeDelta = random.nextLong() % 5 + 8;
          double deltaLat = random.nextDouble() - 0.5;
          double deltaLon = random.nextDouble() - 0.5;
          double desiredStepSize = Math.round(averageMetersPerSecond * timeDelta) * meterToAngle;
          double deltaSize = Math.sqrt(deltaLat * deltaLat + deltaLon * deltaLon);
          ret.add(
              new Position(
                  lastPos.getLatitude() + deltaLat / deltaSize * desiredStepSize,
                  lastPos.getLongitude() + deltaLon / deltaSize * desiredStepSize,
                  lastPos.getTimestamp() + timeDelta));
        }
      }
      return ret;
    }
  }
}
