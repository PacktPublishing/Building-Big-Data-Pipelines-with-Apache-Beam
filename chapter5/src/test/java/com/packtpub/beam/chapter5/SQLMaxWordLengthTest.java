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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;

public class SQLMaxWordLengthTest {

  @Test
  public void testWordLength() {
    Instant now = Instant.now();
    TestStream<String> input =
        TestStream.create(StringUtf8Coder.of())
            .addElements(TimestampedValue.of("a bb ccc d", now))
            .addElements(TimestampedValue.of("eeeee", now.plus(10000)))
            .advanceWatermarkToInfinity();

    Pipeline pipeline = Pipeline.create();
    PCollection<String> strings = pipeline.apply(input);
    PCollection<String> output = SQLMaxWordLength.computeLongestWord(strings);
    PAssert.that(output).containsInAnyOrder("ccc", "eeeee", "eeeee");
    PAssert.that(output).inFinalPane(GlobalWindow.INSTANCE).containsInAnyOrder("eeeee");
    pipeline.run();
  }
}
