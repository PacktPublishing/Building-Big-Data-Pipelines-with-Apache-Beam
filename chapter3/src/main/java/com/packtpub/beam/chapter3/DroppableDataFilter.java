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
package com.packtpub.beam.chapter3;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.packtpub.beam.util.MapToLines;
import java.util.Arrays;
import lombok.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class DroppableDataFilter {

  static final TupleTag<String> mainOutput = new TupleTag<>() {};
  static final TupleTag<String> droppableOutput = new TupleTag<>() {};

  public static void main(String[] args) {
    Params params = parseArgs(args);
    Pipeline pipeline =
        Pipeline.create(PipelineOptionsFactory.fromArgs(params.getRemainingArgs()).create());

    PCollection<String> input =
        readInput(pipeline, params)
            .apply(
                Window.<String>into(FixedWindows.of(Duration.standardMinutes(10)))
                    .withAllowedLateness(Duration.standardSeconds(30))
                    .discardingFiredPanes());
    PCollectionTuple outputs = splitDroppable(mainOutput, droppableOutput, input);
    storeResult(outputs.get(mainOutput), params.getBootstrapServer(), params.getOutputTopicMain());
    storeResult(
        outputs.get(droppableOutput),
        params.getBootstrapServer(),
        params.getOutputTopicDroppable());
    pipeline.run().waitUntilFinish();
  }

  private static PCollection<String> readInput(Pipeline pipeline, Params params) {
    return pipeline
        .apply(
            KafkaIO.<String, String>read()
                .withBootstrapServers(params.getBootstrapServer())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withTimestampPolicyFactory(getTimestampPolicyFactory())
                .withTopic(params.getInputTopic()))
        .apply(MapToLines.ofValuesOnly());
  }

  private static TimestampPolicyFactory<String, String> getTimestampPolicyFactory() {
    return (tp, previousWatermark) ->
        new CustomTimestampPolicyWithLimitedDelay<>(
            record -> Instant.parse(record.getKV().getKey()),
            Duration.standardSeconds(1),
            previousWatermark);
  }

  private static void storeResult(
      PCollection<String> result, String bootstrapServer, String topic) {

    result
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via(e -> KV.of("", e)))
        .apply(
            KafkaIO.<String, String>write()
                .withBootstrapServers(bootstrapServer)
                .withTopic(topic)
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class));
  }

  @VisibleForTesting
  static PCollectionTuple splitDroppable(
      TupleTag<String> mainOutput, TupleTag<String> droppableOutput, PCollection<String> input) {

    @SuppressWarnings("unchecked")
    WindowingStrategy<String, BoundedWindow> strategy =
        (WindowingStrategy<String, BoundedWindow>) input.getWindowingStrategy();

    Coder<BoundedWindow> windowCoder = strategy.getWindowFn().windowCoder();

    PCollectionTuple result =
        input
            .apply(Reify.windows())
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptor.of(BoundedWindow.class), TypeDescriptors.strings()))
                    .via(v -> KV.of(v.getWindow(), v.getValue())))
            .setCoder(KvCoder.of(windowCoder, StringUtf8Coder.of()))
            .apply(Window.into(new GlobalWindows()))
            .apply(
                ParDo.of(
                        new SplitDroppableDataFn(
                            strategy.getAllowedLateness(), mainOutput, droppableOutput))
                    .withOutputTags(mainOutput, TupleTagList.of(droppableOutput)));

    return PCollectionTuple.of(mainOutput, rewindow(result.get(mainOutput), strategy))
        .and(droppableOutput, result.get(droppableOutput));
  }

  private static PCollection<String> rewindow(
      PCollection<String> input, WindowingStrategy<String, BoundedWindow> strategy) {

    if (strategy.getMode() == AccumulationMode.ACCUMULATING_FIRED_PANES) {
      return input.apply(
          Window.into(strategy.getWindowFn())
              .withAllowedLateness(strategy.getAllowedLateness(), strategy.getClosingBehavior())
              .withOnTimeBehavior(strategy.getOnTimeBehavior())
              .withTimestampCombiner(strategy.getTimestampCombiner())
              .triggering(strategy.getTrigger())
              .accumulatingFiredPanes());
    }
    return input.apply(
        Window.into(strategy.getWindowFn())
            .withAllowedLateness(strategy.getAllowedLateness(), strategy.getClosingBehavior())
            .withOnTimeBehavior(strategy.getOnTimeBehavior())
            .withTimestampCombiner(strategy.getTimestampCombiner())
            .triggering(strategy.getTrigger())
            .discardingFiredPanes());
  }

  static Params parseArgs(String[] args) {
    if (args.length < 4) {
      throw new IllegalArgumentException(
          "Expected at least 3 arguments: <bootstrapServer> <inputTopic> <outputTopicRegular> <outputTopicDroppable>");
    }
    return new Params(args[0], args[1], args[2], args[3], Arrays.copyOfRange(args, 4, args.length));
  }

  @Value
  static class Params {
    String bootstrapServer;
    String inputTopic;
    String outputTopicMain;
    String outputTopicDroppable;
    String[] remainingArgs;
  }

  private static class SplitDroppableDataFn extends DoFn<KV<BoundedWindow, String>, String> {

    private final Duration allowedLateness;
    private final TupleTag<String> mainOutput;
    private final TupleTag<String> droppableOutput;

    @StateId("tooLate")
    private final StateSpec<ValueState<Boolean>> tooLateSpec = StateSpecs.value();

    @TimerId("windowGcTimer")
    private final TimerSpec windowGcTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    SplitDroppableDataFn(
        Duration allowedLateness, TupleTag<String> mainOutput, TupleTag<String> droppableOutput) {

      this.allowedLateness = allowedLateness;
      this.mainOutput = mainOutput;
      this.droppableOutput = droppableOutput;
    }

    @ProcessElement
    public void processElement(
        @Element KV<BoundedWindow, String> element,
        @StateId("tooLate") ValueState<Boolean> tooLate,
        @TimerId("windowGcTimer") Timer windowGcTimer,
        MultiOutputReceiver output) {

      boolean tooLateForWindow = MoreObjects.firstNonNull(tooLate.read(), false);
      if (!tooLateForWindow) {
        windowGcTimer.set(element.getKey().maxTimestamp().plus(allowedLateness));
      }
      if (tooLateForWindow) {
        output.get(droppableOutput).output(element.getValue());
      } else {
        output.get(mainOutput).output(element.getValue());
      }
    }

    @OnTimer("windowGcTimer")
    public void onTimer(@StateId("tooLate") ValueState<Boolean> tooLate) {
      tooLate.write(true);
    }
  }
}
