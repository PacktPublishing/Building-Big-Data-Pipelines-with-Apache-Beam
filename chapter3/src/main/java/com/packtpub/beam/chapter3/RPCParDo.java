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
import com.packtpub.beam.chapter3.RpcServiceGrpc.RpcServiceBlockingStub;
import com.packtpub.beam.chapter3.Service.Request;
import com.packtpub.beam.chapter3.Service.Response;
import com.packtpub.beam.util.MapToLines;
import com.packtpub.beam.util.Tokenize;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import lombok.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** A class implementing Task 6 from the book. */
public class RPCParDo {

  public static void main(String[] args) throws IOException {
    Params params = parseArgs(args);
    Pipeline pipeline =
        Pipeline.create(PipelineOptionsFactory.fromArgs(params.getRemainingArgs()).create());

    try (AutoCloseableServer server = AutoCloseableServer.of(runRpc(params.getPort()))) {
      server.getServer().start();
      PCollection<String> input = readInput(pipeline, params);
      PCollection<KV<String, Integer>> result = applyRpc(input, params.getPort());
      storeResult(result, params);
      pipeline.run().waitUntilFinish();
    }
  }

  @VisibleForTesting
  static PCollection<KV<String, Integer>> applyRpc(PCollection<String> input, int port)
      throws UnknownHostException {

    // we need IP address of the pod that will run this code
    // that is due to how name resolution works in K8s
    String hostAddress = InetAddress.getLocalHost().getHostAddress();
    return input.apply(Tokenize.of()).apply(ParDo.of(new RpcDoFn(hostAddress, port)));
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

  private static class RpcDoFn extends DoFn<String, KV<String, Integer>> {

    private final String hostname;
    private final int port;

    // channel and stub are not Serializable, we don't want to serialize them, we create them
    // in @Setup method instead
    private transient ManagedChannel channel;
    private transient RpcServiceBlockingStub stub;

    RpcDoFn(String hostname, int port) {
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
    public void process(@Element String input, OutputReceiver<KV<String, Integer>> output) {
      Response response = stub.resolve(Request.newBuilder().setInput(input).build());
      output.output(KV.of(input, response.getOutput()));
    }
  }

  static Params parseArgs(String[] args) {
    if (args.length < 3) {
      throw new IllegalArgumentException(
          "Expected at least 3 arguments: <bootstrapServer> <inputTopic> <outputTopic>");
    }
    int port = 1234;
    int extraArgs = 3;
    if (args.length > 4 && args[3].equals("--port")) {
      port = Integer.parseInt(args[4]);
      extraArgs = 5;
    }
    return new Params(
        args[0], args[1], args[2], port, Arrays.copyOfRange(args, extraArgs, args.length));
  }

  @Value
  static class Params {
    String bootstrapServer;
    String inputTopic;
    String outputTopic;
    int port;
    String[] remainingArgs;
  }
}
