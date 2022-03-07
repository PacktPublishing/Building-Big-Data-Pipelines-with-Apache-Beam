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
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.packtpub.beam.chapter3.RpcServiceGrpc.RpcServiceBlockingStub;
import com.packtpub.beam.chapter3.Service.Request;
import com.packtpub.beam.chapter3.Service.RequestList;
import com.packtpub.beam.chapter3.Service.Response;
import com.packtpub.beam.chapter3.Service.ResponseList;
import com.packtpub.beam.util.MapToLines;
import com.packtpub.beam.util.Tokenize;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
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
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Task 8 from the book. */
public class RPCParDoStateful {

  public static void main(String[] args) throws IOException {
    Params params = parseArgs(args);
    Pipeline pipeline =
        Pipeline.create(PipelineOptionsFactory.fromArgs(params.getRemainingArgs()).create());

    try (AutoCloseableServer server = AutoCloseableServer.of(runRpc(params.getPort()))) {
      server.getServer().start();
      PCollection<String> input = readInput(pipeline, params);
      PCollection<KV<String, Integer>> result = applyRpc(input, params);
      storeResult(result, params);
      pipeline.run().waitUntilFinish();
    }
  }

  @VisibleForTesting
  static PCollection<KV<String, Integer>> applyRpc(PCollection<String> input, Params params)
      throws UnknownHostException {

    // we need IP address of the pod that will run this code
    // that is due to how name resolution works in K8s
    String hostAddress = InetAddress.getLocalHost().getHostAddress();
    return input
        .apply(Tokenize.of())
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                .via(e -> KV.of((e.hashCode() & Integer.MAX_VALUE) % 10, e)))
        .apply(
            ParDo.of(
                new BatchRpcDoFnStateful(
                    params.getBatchSize(),
                    params.getMaxWaitTime(),
                    hostAddress,
                    params.getPort())));
  }

  private static PCollection<String> readInput(Pipeline pipeline, Params params) {
    return pipeline
        .apply(
            KafkaIO.<String, String>read()
                .withBootstrapServers(params.getBootstrapServer())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withTopic(params.getInputTopic()))
        .apply(MapToLines.of());
  }

  private static void storeResult(PCollection<KV<String, Integer>> result, Params params) {
    result
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via(e -> KV.of("", e.getKey() + " " + e.getValue())))
        .apply(
            KafkaIO.<String, String>write()
                .withBootstrapServers(params.getBootstrapServer())
                .withTopic(params.getOutputTopic())
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class));
  }

  @VisibleForTesting
  static Server runRpc(int port) {
    return ServerBuilder.forPort(port).addService(new RPCService()).build();
  }

  @Value
  private static class ValueWithTimestamp<T> {
    T value;
    Instant timestamp;
  }

  private static class ValueWithTimestampCoder<T> extends CustomCoder<ValueWithTimestamp<T>> {

    private static final InstantCoder INSTANT_CODER = InstantCoder.of();

    @SuppressWarnings("unchecked")
    public static <T> ValueWithTimestampCoder<T> of(Coder<T> valueCoder) {
      return new ValueWithTimestampCoder<>(valueCoder);
    }

    private final Coder<T> valueCoder;

    private ValueWithTimestampCoder(Coder<T> valueCoder) {
      this.valueCoder = valueCoder;
    }

    @Override
    public void encode(ValueWithTimestamp<T> value, OutputStream outStream)
        throws CoderException, IOException {

      valueCoder.encode(value.getValue(), outStream);
      INSTANT_CODER.encode(value.getTimestamp(), outStream);
    }

    @Override
    public ValueWithTimestamp<T> decode(InputStream inStream) throws CoderException, IOException {
      return new ValueWithTimestamp<>(valueCoder.decode(inStream), INSTANT_CODER.decode(inStream));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.singletonList(valueCoder);
    }
  }

  private static class BatchRpcDoFnStateful extends DoFn<KV<Integer, String>, KV<String, Integer>> {

    private final int maxBatchSize;
    private final Duration maxBatchWait;
    private final String hostname;
    private final int port;

    // channel and stub are not Serializable, we don't want to serialize them, we create them
    // in @Setup method instead
    private transient ManagedChannel channel;
    private transient RpcServiceBlockingStub stub;

    @StateId("batch")
    private final StateSpec<BagState<ValueWithTimestamp<String>>> batchSpec =
        StateSpecs.bag(ValueWithTimestampCoder.of(StringUtf8Coder.of()));

    @StateId("batchSize")
    private final StateSpec<ValueState<Integer>> batchSizeSpec = StateSpecs.value();

    @TimerId("flushTimer")
    private final TimerSpec flushTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @TimerId("endOfTime")
    private final TimerSpec endOfTimeTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    BatchRpcDoFnStateful(int maxBatchSize, Duration maxBatchWait, String hostname, int port) {
      this.maxBatchSize = maxBatchSize;
      this.maxBatchWait = maxBatchWait;
      this.hostname = hostname;
      this.port = port;
    }

    @Setup
    public void setup() {
      channel = ManagedChannelBuilder.forAddress(hostname, port).usePlaintext().build();
      stub = RpcServiceGrpc.newBlockingStub(channel);
    }

    @Teardown
    public void tearDown() {
      // ideally, we want tearDown to be idempotent
      if (channel != null) {
        channel.shutdownNow();
        channel = null;
      }
    }

    @ProcessElement
    public void process(
        @Element KV<Integer, String> input,
        @Timestamp Instant timestamp,
        @StateId("batch") BagState<ValueWithTimestamp<String>> elements,
        @StateId("batchSize") ValueState<Integer> batchSize,
        @TimerId("flushTimer") Timer flushTimer,
        @TimerId("endOfTime") Timer endOfTimeTimer,
        OutputReceiver<KV<String, Integer>> outputReceiver) {

      endOfTimeTimer.set(GlobalWindow.INSTANCE.maxTimestamp());
      int currentSize = MoreObjects.firstNonNull(batchSize.read(), 0);
      ValueWithTimestamp<String> value = new ValueWithTimestamp<>(input.getValue(), timestamp);
      if (currentSize == maxBatchSize - 1) {
        flushOutput(
            Iterables.concat(elements.read(), Collections.singletonList(value)), outputReceiver);
        clearState(elements, batchSize, flushTimer);
      } else {
        if (currentSize == 0) {
          flushTimer.offset(maxBatchWait).setRelative();
        }
        elements.add(value);
        batchSize.write(currentSize + 1);
      }
    }

    @OnTimer("flushTimer")
    public void onFlushTimer(
        @StateId("batch") BagState<ValueWithTimestamp<String>> elements,
        @StateId("batchSize") ValueState<Integer> batchSize,
        OutputReceiver<KV<String, Integer>> outputReceiver) {

      flushOutput(elements.read(), outputReceiver);
      clearState(elements, batchSize, null);
    }

    @OnTimer("endOfTime")
    public void onEndOfTimeTimer(
        @StateId("batch") BagState<ValueWithTimestamp<String>> elements,
        @StateId("batchSize") ValueState<Integer> batchSize,
        OutputReceiver<KV<String, Integer>> outputReceiver) {

      flushOutput(elements.read(), outputReceiver);
      // no need to clear state, will be cleaned by the runner
    }

    private void clearState(
        BagState<ValueWithTimestamp<String>> elements,
        ValueState<Integer> batchSize,
        @Nullable Timer flushTimer) {

      elements.clear();
      batchSize.clear();
      if (flushTimer != null) {
        // Beam is currently missing a clear() method for Timer
        // see https://issues.apache.org/jira/browse/BEAM-10887 for updates
        flushTimer.offset(maxBatchWait).setRelative();
      }
    }

    private void flushOutput(
        Iterable<ValueWithTimestamp<String>> elements,
        OutputReceiver<KV<String, Integer>> outputReceiver) {

      Map<String, List<ValueWithTimestamp<String>>> distinctElements =
          Streams.stream(elements)
              .collect(Collectors.groupingBy(ValueWithTimestamp::getValue, Collectors.toList()));
      RequestList.Builder builder = RequestList.newBuilder();
      distinctElements.keySet().forEach(r -> builder.addRequest(Request.newBuilder().setInput(r)));
      RequestList requestList = builder.build();
      ResponseList responseList = stub.resolveBatch(requestList);
      Preconditions.checkArgument(requestList.getRequestCount() == responseList.getResponseCount());
      for (int i = 0; i < requestList.getRequestCount(); i++) {
        Request request = requestList.getRequest(i);
        Response response = responseList.getResponse(i);
        List<ValueWithTimestamp<String>> timestampsAndWindows =
            distinctElements.get(request.getInput());
        KV<String, Integer> value = KV.of(request.getInput(), response.getOutput());
        timestampsAndWindows.forEach(
            v -> outputReceiver.outputWithTimestamp(value, v.getTimestamp()));
      }
    }
  }

  static Params parseArgs(String[] args) {
    if (args.length < 3) {
      throw new IllegalArgumentException(
          "Expected at least 3 arguments: <bootstrapServer> <inputTopic> <outputTopic>");
    }
    int port = 1234;
    int extraArgs = 3;
    int batchSize = 100;
    Duration maxWaitTime = Duration.millis(100);
    loop:
    while (args.length > extraArgs + 1) {
      switch (args[extraArgs]) {
        case "--port":
          port = Integer.parseInt(args[extraArgs + 1]);
          break;
        case "--batchSize":
          batchSize = Integer.parseInt(args[extraArgs + 1]);
          break;
        case "--maxWaitMs":
          maxWaitTime = Duration.millis(Integer.parseInt(args[extraArgs + 1]));
          break;
        default:
          break loop;
      }
      extraArgs += 2;
    }
    return new Params(
        args[0],
        args[1],
        args[2],
        port,
        batchSize,
        maxWaitTime,
        Arrays.copyOfRange(args, extraArgs, args.length));
  }

  @Value
  static class Params {
    String bootstrapServer;
    String inputTopic;
    String outputTopic;
    int port;
    int batchSize;
    Duration maxWaitTime;
    String[] remainingArgs;
  }
}
