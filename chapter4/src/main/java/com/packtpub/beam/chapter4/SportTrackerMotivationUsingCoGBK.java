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
package com.packtpub.beam.chapter4;

import com.packtpub.beam.util.Position;
import com.packtpub.beam.util.ReadPositionsFromKafka;
import com.packtpub.beam.util.ToMetric;
import com.packtpub.beam.util.ToMetric.Metric;
import com.packtpub.beam.util.WriteNotificationsToKafka;
import java.util.Arrays;
import java.util.Collections;
import lombok.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

public class SportTrackerMotivationUsingCoGBK
    extends PTransform<PCollection<KV<String, Position>>, PCollection<KV<String, Boolean>>> {

  @Override
  public PCollection<KV<String, Boolean>> expand(PCollection<KV<String, Position>> input) {
    PCollection<KV<String, Metric>> metrics = input.apply("computeMetrics", new ToMetric());

    PCollection<KV<String, Double>> longAverage =
        metrics
            .apply(
                Window.into(
                    SlidingWindows.of(Duration.standardMinutes(5))
                        .every(Duration.standardMinutes(1))))
            .apply("longAverage", new ComputeAverage())
            .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));

    PCollection<KV<String, Double>> shortAverage =
        metrics
            .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
            .apply("shortAverage", new ComputeAverage());

    TupleTag<Double> shortTag = new TupleTag<>("short");
    TupleTag<Double> longTag = new TupleTag<>("long");
    return KeyedPCollectionTuple.of(longTag, longAverage)
        .and(shortTag, shortAverage)
        .apply(CoGroupByKey.create())
        .apply(
            FlatMapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.booleans()))
                .via(
                    elem -> {
                      double longPace = elem.getValue().getOnly(longTag, 0.0);
                      double shortPace = elem.getValue().getOnly(shortTag, 0.0);
                      if (longPace > 0.0 && shortPace > 0.0) {
                        double ratio = shortPace / longPace;
                        if (ratio < 0.9 || ratio > 1.1) {
                          return Collections.singletonList(KV.of(elem.getKey(), ratio > 1.0));
                        }
                      }
                      return Collections.emptyList();
                    }));
  }

  public static void main(String[] args) {
    SportTrackerMotivationUsingSideInputs.Params params = parseArgs(args);
    Pipeline pipeline =
        Pipeline.create(PipelineOptionsFactory.fromArgs(params.getRemainingArgs()).create());

    pipeline
        .apply(
            "ReadRecords",
            new ReadPositionsFromKafka(params.getBootstrapServer(), params.getInputTopic()))
        .apply("computeTrackNotifications", new SportTrackerMotivationUsingSideInputs())
        .apply(
            "createUserNotification",
            new WriteNotificationsToKafka(params.getBootstrapServer(), params.getOutputTopic()));

    pipeline.run().waitUntilFinish();
  }

  static SportTrackerMotivationUsingSideInputs.Params parseArgs(String[] args) {
    if (args.length < 3) {
      throw new IllegalArgumentException(
          "Expected at least 3 arguments: <bootstrapServer> <inputTopic> <outputTopic>");
    }
    return new SportTrackerMotivationUsingSideInputs.Params(
        args[0], args[1], args[2], Arrays.copyOfRange(args, 3, args.length));
  }

  @Value
  static class Params {
    String bootstrapServer;
    String inputTopic;
    String outputTopic;
    String[] remainingArgs;
  }
}
