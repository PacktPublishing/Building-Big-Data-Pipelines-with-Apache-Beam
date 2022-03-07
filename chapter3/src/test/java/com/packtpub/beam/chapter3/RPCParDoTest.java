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

import static com.packtpub.beam.chapter3.RPCParDo.runRpc;
import static com.packtpub.beam.util.Utils.getLines;
import static com.packtpub.beam.util.Utils.toWords;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Test {@link RPCParDo}. */
public class RPCParDoTest {

  private static final int PORT = 1234;
  private static AutoCloseableServer server;

  @BeforeAll
  public static void setup() throws IOException {
    server = AutoCloseableServer.of(runRpc(PORT));
    server.getServer().start();
  }

  @AfterAll
  public static void teardown() {
    server.close();
  }

  @Test
  public void testRpc() throws IOException {
    List<String> input = getLines(getClass().getClassLoader().getResourceAsStream("lorem.txt"));
    Pipeline pipeline = Pipeline.create();
    PCollection<String> lines = pipeline.apply(Create.of(input));
    PCollection<KV<String, Integer>> result = RPCParDo.applyRpc(lines, PORT);
    List<KV<String, Integer>> expectedResult =
        input
            .stream()
            .flatMap(l -> toWords(l).stream())
            .map(s -> KV.of(s, s.length()))
            .collect(Collectors.toList());
    PAssert.that(result).containsInAnyOrder(expectedResult);
    pipeline.run().waitUntilFinish();
  }
}
