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
package com.packtpub.beam.chapter1;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.packtpub.beam.util.PrintElements;
import com.packtpub.beam.util.Tokenize;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;

public class Chapter1DemoTest {

  List<String> lines;

  public Chapter1DemoTest() throws Exception {
    lines =
        Files.readAllLines(
            Paths.get(ClassLoader.getSystemClassLoader().getResource("lorem.txt").getFile()),
            StandardCharsets.UTF_8);
  }

  @Test
  public void testFirstPipeline() {
    Pipeline pipeline = Pipeline.create();
    PCollection<String> input = pipeline.apply(Create.of(lines));
    PCollection<String> words = input.apply(Tokenize.of());
    PCollection<KV<String, Long>> result = words.apply(Count.perElement());
    result.apply(PrintElements.of());
    assertNotNull(pipeline.run());
  }

  @Test
  public void testFirstPipelineChained() {
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply(Create.of(lines))
        .apply(Tokenize.of())
        .apply(Count.perElement())
        .apply(PrintElements.of());
    assertNotNull(pipeline.run());
  }

  @Test
  public void testFirstPipelineStreamMissingWindow() {
    Pipeline pipeline = Pipeline.create();
    TestStream.Builder<String> streamBuilder = TestStream.create(StringUtf8Coder.of());
    Instant now = Instant.now();
    IntStream.range(0, lines.size())
        .mapToObj(i -> TimestampedValue.of(lines.get(i), now.plus(i)))
        .forEach(streamBuilder::addElements);
    PCollection<String> input = pipeline.apply(streamBuilder.advanceWatermarkToInfinity());
    PCollection<String> words = input.apply(Tokenize.of());
    assertThrows(IllegalStateException.class, () -> words.apply(Count.perElement()));
  }

  @Test
  public void testFirstPipelineStream() {
    Pipeline pipeline = Pipeline.create();
    TestStream.Builder<String> streamBuilder = TestStream.create(StringUtf8Coder.of());
    Instant now = Instant.now();
    IntStream.range(0, lines.size())
        .mapToObj(i -> TimestampedValue.of(lines.get(i), now.plus(i)))
        .forEach(streamBuilder::addElements);
    PCollection<String> input = pipeline.apply(streamBuilder.advanceWatermarkToInfinity());
    PCollection<String> words = input.apply(Tokenize.of());
    PCollection<String> windowed =
        words.apply(
            Window.<String>into(new GlobalWindows())
                .discardingFiredPanes()
                .triggering(AfterWatermark.pastEndOfWindow()));
    PCollection<KV<String, Long>> result = windowed.apply(Count.perElement());
    result.apply(PrintElements.of());
    assertNotNull(pipeline.run());
  }

  @Test
  public void testFirstPipelineStreamSimplified() {
    Pipeline pipeline = Pipeline.create();
    TestStream.Builder<String> streamBuilder = TestStream.create(StringUtf8Coder.of());
    Instant now = Instant.now();
    IntStream.range(0, lines.size())
        .mapToObj(i -> TimestampedValue.of(lines.get(i), now.plus(i)))
        .forEach(streamBuilder::addElements);

    pipeline
        .apply(streamBuilder.advanceWatermarkToInfinity())
        .apply(Tokenize.of())
        .apply(
            Window.<String>into(new GlobalWindows())
                .discardingFiredPanes()
                .triggering(AfterWatermark.pastEndOfWindow()))
        .apply(Count.perElement())
        .apply(PrintElements.of());

    assertNotNull(pipeline.run());
  }

  @Test
  public void testTopNPipeline() {
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply(Create.of(lines))
        .apply(Tokenize.of())
        .apply(Count.perElement())
        .apply(
            Top.of(
                5,
                (Comparator<KV<String, Long>> & Serializable)
                    (a, b) -> Long.compare(a.getValue(), b.getValue())))
        .apply(PrintElements.of());
    assertNotNull(pipeline.run());
  }
}
