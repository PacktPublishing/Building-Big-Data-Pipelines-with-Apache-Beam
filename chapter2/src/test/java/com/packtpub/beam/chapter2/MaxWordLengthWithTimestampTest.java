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
package com.packtpub.beam.chapter2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;

public class MaxWordLengthWithTimestampTest {

  @Test
  public void testWordLength() {
    Instant now = Instant.now();
    TestStream<String> input =
        TestStream.create(StringUtf8Coder.of())
            .addElements(TimestampedValue.of("a", now))
            .addElements(TimestampedValue.of("bb", now.plus(10000)))
            .addElements(TimestampedValue.of("ccc", now.plus(20000)))
            .addElements(TimestampedValue.of("d", now.plus(30000)))
            .addElements(TimestampedValue.of("ccc", now.plus(40000)))
            .advanceWatermarkToInfinity();

    Pipeline pipeline = Pipeline.create();
    PCollection<String> strings = pipeline.apply(input);
    PCollection<KV<String, String>> output =
        MaxWordLengthWithTimestamp.computeLongestWordWithTimestamp(strings);
    PAssert.that(output)
        .containsInAnyOrder(
            KV.of(now.toString(), "a"),
            KV.of(now.plus(10000).toString(), "bb"),
            KV.of(now.plus(20000).toString(), "ccc"),
            KV.of(now.plus(30000).toString(), "ccc"),
            KV.of(now.plus(40000).toString(), "ccc"),
            KV.of(GlobalWindow.INSTANCE.maxTimestamp().toString(), "ccc"));
    pipeline.run();
  }
}
