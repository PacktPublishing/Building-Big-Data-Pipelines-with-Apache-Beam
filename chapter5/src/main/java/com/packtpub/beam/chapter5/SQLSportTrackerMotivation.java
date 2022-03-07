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

import com.packtpub.beam.util.Position;
import com.packtpub.beam.util.ReadPositionsFromKafka;
import com.packtpub.beam.util.ToMetric;
import com.packtpub.beam.util.ToMetric.Metric;
import com.packtpub.beam.util.WriteNotificationsToKafka;
import java.util.Arrays;
import lombok.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

public class SQLSportTrackerMotivation
    extends PTransform<PCollection<KV<String, Position>>, PCollection<KV<String, Boolean>>> {

  private static final Schema METRIC_SCHEMA =
      Schema.of(
          Field.of("workoutId", FieldType.STRING),
          Field.of("length", FieldType.DOUBLE),
          Field.of("duration", FieldType.INT64),
          Field.of("ts", FieldType.DATETIME));

  @Override
  public PCollection<KV<String, Boolean>> expand(PCollection<KV<String, Position>> input) {
    PCollection<KV<String, Metric>> metrics =
        input
            .apply("computeMetrics", new ToMetric())
            .apply(Reify.timestamps())
            .setSchema(
                METRIC_SCHEMA,
                (TypeDescriptor) TypeDescriptor.of(TimestampedValue.class),
                kv ->
                    Row.withSchema(METRIC_SCHEMA)
                        .attachValues(
                            kv.getValue().getKey(),
                            kv.getValue().getValue().getLength(),
                            kv.getValue().getValue().getDuration(),
                            kv.getTimestamp()),
                r ->
                    TimestampedValue.of(
                        KV.of(r.getString(0), new Metric(r.getDouble(1), r.getInt64(2))),
                        r.getDateTime(3).toInstant()));

    PCollection<Row> rows = metrics.apply(Convert.toRows());
    PCollection<Row> longAverage =
        rows.apply(
                "longAverage",
                SqlTransform.query(
                    "SELECT workoutId, SUM(length) / SUM(duration) longPace FROM PCOLLECTION GROUP BY "
                        + "workoutId, HOP(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)"))
            .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));

    PCollection<Row> shortAverage =
        rows.apply(
            "shortAverage",
            SqlTransform.query(
                "SELECT workoutId, SUM(length) / SUM(duration) shortPace FROM PCOLLECTION GROUP BY "
                    + "workoutId, TUMBLE(ts, INTERVAL '1' MINUTE)"));

    PCollection<Row> joined =
        PCollectionTuple.of("longAverage", longAverage)
            .and("shortAverage", shortAverage)
            .apply(
                "shortAndLongAverageJoin",
                SqlTransform.query(
                    "SELECT longAverage.workoutId workoutId, longPace, shortPace "
                        + "FROM longAverage NATURAL JOIN shortAverage"))
            .apply(
                "filterRelevantChanges",
                SqlTransform.query(
                    "WITH ratios AS ("
                        + "SELECT workoutId, shortPace / longPace AS ratio FROM PCOLLECTION)\n"
                        + "SELECT workoutId, ratio > 1 FROM ratios WHERE ratio < 0.9 OR ratio > 1.1"));

    return joined.apply(
        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.booleans()))
            .via(r -> KV.of(r.getString(0), r.getBoolean(1))));
  }

  public static void main(String[] args) {
    Params params = parseArgs(args);
    Pipeline pipeline =
        Pipeline.create(PipelineOptionsFactory.fromArgs(params.getRemainingArgs()).create());

    pipeline
        .apply(
            "ReadRecords",
            new ReadPositionsFromKafka(params.getBootstrapServer(), params.getInputTopic()))
        .apply("computeTrackNotifications", new SQLSportTrackerMotivation())
        .apply(
            "createUserNotification",
            new WriteNotificationsToKafka(params.getBootstrapServer(), params.getOutputTopic()));

    pipeline.run().waitUntilFinish();
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
