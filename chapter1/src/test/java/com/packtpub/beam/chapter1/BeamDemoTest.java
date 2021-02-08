/**
 * Copyright 2021-2021 Packt Publishing Limited
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
package com.packtpub.beam.chapter1;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.packtpub.beam.util.LogResults;
import com.packtpub.beam.util.Tokenize;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;

public class BeamDemoTest {

  List<String> lines;

  public BeamDemoTest() throws Exception {
    lines =
        Files.readAllLines(
            Paths.get(ClassLoader.getSystemClassLoader().getResource("shakespeare.txt").getFile()),
            StandardCharsets.UTF_8);
  }

  @Test
  public void testFirstPipeline() {
    Pipeline pipeline = Pipeline.create();
    PCollection<String> input = pipeline.apply(Create.of(lines));
    PCollection<String> words = input.apply(Tokenize.of());
    PCollection<KV<String, Long>> result = words.apply(Count.perElement());
    result.apply(LogResults.of());
    assertNotNull(pipeline.run());
  }
}
