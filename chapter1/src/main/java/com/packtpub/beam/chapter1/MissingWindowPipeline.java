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

import com.packtpub.beam.util.Tokenize;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;

public class MissingWindowPipeline {

  public static void main(String[] args) throws IOException {

    ClassLoader loader = Chapter1Demo.class.getClassLoader();
    String file = loader.getResource("lorem.txt").getFile();
    List<String> lines = Files.readAllLines(Paths.get(file), StandardCharsets.UTF_8);

    // create Pipeline
    Pipeline pipeline = Pipeline.create();

    // create a 'TestStream' - a utility that emulates
    // unbounded data source
    TestStream.Builder<String> streamBuilder = TestStream.create(StringUtf8Coder.of());

    // we need a timestamp for each data element
    Instant now = Instant.now();

    // add all lines with timestamps to the TestStream
    List<TimestampedValue<String>> timestamped =
        IntStream.range(0, lines.size())
            .mapToObj(i -> TimestampedValue.of(lines.get(i), now.plus(i)))
            .collect(Collectors.toList());

    for (TimestampedValue<String> value : timestamped) {
      streamBuilder = streamBuilder.addElements(value);
    }

    // create the unbounded PCollection from TestStream
    PCollection<String> input = pipeline.apply(streamBuilder.advanceWatermarkToInfinity());

    PCollection<String> words = input.apply(Tokenize.of());
    words.apply(Count.perElement());
  }
}
