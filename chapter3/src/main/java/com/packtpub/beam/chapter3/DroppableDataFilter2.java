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
import com.packtpub.beam.util.MapToLines;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import lombok.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
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

public class DroppableDataFilter2 {

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

  @SuppressWarnings({"unchecked", "rawtypes"})
  @VisibleForTesting
  static PCollectionTuple splitDroppable(
      TupleTag<String> mainOutput, TupleTag<String> droppableOutput, PCollection<String> input) {

    WindowingStrategy<String, BoundedWindow> strategy =
        (WindowingStrategy<String, BoundedWindow>) input.getWindowingStrategy();

    Coder<BoundedWindow> windowCoder = strategy.getWindowFn().windowCoder();
    final Duration loopDuration;
    if ((WindowFn) strategy.getWindowFn() instanceof FixedWindows) {
      FixedWindows fn = (FixedWindows) (WindowFn) strategy.getWindowFn();
      loopDuration = fn.getSize().getMillis() > 5000 ? Duration.standardSeconds(5) : fn.getSize();
    } else {
      loopDuration = Duration.standardSeconds(1);
    }

    PCollection<String> impulseString =
        input
            .getPipeline()
            .apply(Impulse.create())
            .apply(MapElements.into(TypeDescriptors.strings()).via(e -> ""));

    PCollection<String> inputWatermarkOnly =
        input.apply(Window.into(new GlobalWindows())).apply(Filter.by(e -> false));

    PCollection<KV<BoundedWindow, String>> labelsWithoutInput =
        PCollectionList.of(impulseString)
            .and(inputWatermarkOnly)
            .apply(Flatten.pCollections())
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                    .via(e -> KV.of("", e)))
            .apply(
                ParDo.of(
                    new LoopTimerForWindowLabels(
                        loopDuration,
                        Instant.now().minus(Duration.standardHours(1)),
                        (WindowingStrategy) strategy)))
            .setCoder(windowCoder)
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptor.of(BoundedWindow.class), TypeDescriptors.strings()))
                    .via(e -> KV.of(e, "")))
            .setCoder(KvCoder.of(windowCoder, StringUtf8Coder.of()));

    PCollection<KV<BoundedWindow, String>> labelWithInput =
        input
            .apply(Reify.windows())
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptor.of(BoundedWindow.class), TypeDescriptors.strings()))
                    .via(v -> KV.of(v.getWindow(), v.getValue())))
            .setCoder(KvCoder.of(windowCoder, StringUtf8Coder.of()))
            .apply(Window.into(new GlobalWindows()));

    PCollectionTuple result =
        PCollectionList.of(labelsWithoutInput)
            .and(labelWithInput)
            .apply(Flatten.pCollections())
            .apply(
                ParDo.of(
                        new SplitDroppableDataFn(
                            strategy.getAllowedLateness(),
                            windowCoder,
                            mainOutput,
                            droppableOutput))
                    .withOutputTags(mainOutput, TupleTagList.of(droppableOutput)));

    return PCollectionTuple.of(mainOutput, rewindow(result.get(mainOutput), strategy))
        .and(droppableOutput, result.get(droppableOutput));
  }

  private static <T> PCollection<T> rewindow(
      PCollection<T> input, WindowingStrategy<T, BoundedWindow> strategy) {

    if (strategy.getMode() == AccumulationMode.ACCUMULATING_FIRED_PANES) {
      return input.apply(
          Window.<T>into(strategy.getWindowFn())
              .withAllowedLateness(strategy.getAllowedLateness(), strategy.getClosingBehavior())
              .withOnTimeBehavior(strategy.getOnTimeBehavior())
              .withTimestampCombiner(strategy.getTimestampCombiner())
              .triggering(strategy.getTrigger())
              .accumulatingFiredPanes());
    }
    return input.apply(
        Window.<T>into(strategy.getWindowFn())
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
    private final Coder<BoundedWindow> windowCoder;
    private final TupleTag<String> mainOutput;
    private final TupleTag<String> droppableOutput;

    @StateId("buffer")
    private final StateSpec<BagState<KV<Instant, String>>> bufferSpec =
        StateSpecs.bag(KvCoder.of(InstantCoder.of(), StringUtf8Coder.of()));

    @TimerId("bufferFlush")
    private final TimerSpec bufferFlushTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @StateId("windowClosed")
    private final StateSpec<ValueState<Boolean>> windowClosedSpec = StateSpecs.value();

    @TimerId("windowGcTimer")
    private final TimerSpec windowGcTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    SplitDroppableDataFn(
        Duration allowedLateness,
        Coder<BoundedWindow> windowCoder,
        TupleTag<String> mainOutput,
        TupleTag<String> droppableOutput) {

      this.allowedLateness = allowedLateness;
      this.windowCoder = windowCoder;
      this.mainOutput = mainOutput;
      this.droppableOutput = droppableOutput;
    }

    @Override
    public Duration getAllowedTimestampSkew() {
      return Duration.standardHours(1);
    }

    @ProcessElement
    public void processElement(
        @Element KV<BoundedWindow, String> element,
        @StateId("windowClosed") ValueState<Boolean> windowClosed,
        @StateId("buffer") BagState<KV<Instant, String>> buffer,
        @TimerId("windowGcTimer") Timer windowGcTimer,
        @TimerId("bufferFlush") Timer bufferFlush,
        @Timestamp Instant ts,
        MultiOutputReceiver output) {

      buffer.readLater();
      Boolean isWindowClosed = windowClosed.read();
      if (isWindowClosed == null) {
        // we have not had a timer firing yet
        // we cannot conclude about lateness of elements, so we buffer them
        Instant lastNotDroppable = element.getKey().maxTimestamp().plus(allowedLateness);
        Instant gcTime = lastNotDroppable.plus(1);
        windowGcTimer.withOutputTimestamp(lastNotDroppable).set(gcTime);
        bufferFlush.offset(Duration.ZERO).setRelative();
        buffer.add(KV.of(ts, element.getValue()));
      } else {
        flushOutput(element.getValue(), ts, isWindowClosed, output);
      }
    }

    @OnTimer("windowGcTimer")
    public void onTimer(
        @StateId("windowClosed") ValueState<Boolean> windowClosed,
        @StateId("buffer") BagState<KV<Instant, String>> buffer,
        MultiOutputReceiver output) {

      windowClosed.write(true);
      flushBuffer(buffer, true, output);
    }

    @OnTimer("bufferFlush")
    public void onFlushTimer(
        OnTimerContext context,
        @Key BoundedWindow window,
        @StateId("buffer") BagState<KV<Instant, String>> buffer,
        @StateId("windowClosed") ValueState<Boolean> windowClosed,
        MultiOutputReceiver output) {

      Boolean isWindowClosed = windowClosed.read();
      final boolean closed;
      if (isWindowClosed == null) {
        closed = window.maxTimestamp().plus(allowedLateness).isBefore(context.fireTimestamp());
        windowClosed.write(closed);
      } else {
        closed = isWindowClosed;
      }
      flushBuffer(buffer, closed, output);
    }

    private void flushOutput(
        String element, Instant timestamp, boolean closed, MultiOutputReceiver output) {
      if (!element.isBlank()) {
        if (closed) {
          output.get(droppableOutput).outputWithTimestamp(element, timestamp);
        } else {
          output.get(mainOutput).outputWithTimestamp(element, timestamp);
        }
      }
    }

    private void flushBuffer(
        BagState<KV<Instant, String>> buffer, boolean closed, MultiOutputReceiver output) {

      for (KV<Instant, String> buffered : buffer.read()) {
        flushOutput(buffered.getValue(), buffered.getKey(), closed, output);
      }
      buffer.clear();
    }
  }

  private static class LoopTimerForWindowLabels extends DoFn<KV<String, String>, BoundedWindow> {

    private final Duration loopDuration;
    private final Instant startingTime;
    private final WindowingStrategy<KV<String, String>, BoundedWindow> windowingStrategy;

    @TimerId("loopTimer")
    private final TimerSpec loopTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @StateId("lastFireStamp")
    private final StateSpec<ValueState<Instant>> lastFireStampSpec = StateSpecs.value();

    @SuppressWarnings("unchecked")
    public LoopTimerForWindowLabels(
        Duration loopDuration,
        Instant startingTime,
        WindowingStrategy<KV<String, String>, ?> windowingStrategy) {

      this.loopDuration = loopDuration;
      this.startingTime = startingTime;
      this.windowingStrategy =
          (WindowingStrategy<KV<String, String>, BoundedWindow>) windowingStrategy;
    }

    @ProcessElement
    public void process(
        @Element KV<String, String> input,
        @TimerId("loopTimer") Timer loopTimer,
        @StateId("lastFireStamp") ValueState<Instant> lastFireStamp) {

      lastFireStamp.write(startingTime);
      loopTimer.set(startingTime);
    }

    @OnTimer("loopTimer")
    public void onTimer(
        @TimerId("loopTimer") Timer loopTimer,
        @StateId("lastFireStamp") ValueState<Instant> lastFireStamp,
        OnTimerContext context,
        OutputReceiver<BoundedWindow> output)
        throws Exception {

      Instant lastFired = Objects.requireNonNull(lastFireStamp.read());
      Instant timestamp = context.fireTimestamp();
      lastFireStamp.write(timestamp);
      if (timestamp.isBefore(GlobalWindow.INSTANCE.maxTimestamp())) {
        loopTimer.withOutputTimestamp(timestamp).offset(loopDuration).setRelative();
        outputWindowsWithinInterval(output, lastFired, timestamp);
      }
    }

    private void outputWindowsWithinInterval(
        OutputReceiver<BoundedWindow> output, Instant minStamp, Instant maxStamp) throws Exception {

      Instant timestamp = minStamp;
      while (timestamp.isBefore(maxStamp)) {
        Collection<BoundedWindow> windows =
            windowingStrategy
                .getWindowFn()
                .assignWindows(newContext(windowingStrategy.getWindowFn(), timestamp));
        windows.forEach(output::output);
        timestamp = timestamp.plus(loopDuration);
      }
    }

    private <T> WindowFn<T, BoundedWindow>.AssignContext newContext(
        WindowFn<T, BoundedWindow> windowFn, Instant timestamp) {

      return windowFn.new AssignContext() {

        @Override
        public T element() {
          return null;
        }

        @Override
        public Instant timestamp() {
          return timestamp;
        }

        @Override
        public BoundedWindow window() {
          return null;
        }
      };
    }
  }
}
