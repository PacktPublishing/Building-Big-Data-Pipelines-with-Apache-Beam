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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;

public class TopKWordsTest {

  @Test
  public void testComputeStatsInWindows() {
    Duration windowDuration = Duration.standardSeconds(10);
    Instant now = Instant.now();
    Instant startOfTenSecondWindow = now.plus(-now.getMillis() % windowDuration.getMillis());
    TestStream<String> lines = createInput(startOfTenSecondWindow);
    Pipeline pipeline = Pipeline.create();
    PCollection<String> input = pipeline.apply(lines);
    PCollection<KV<String, Long>> output =
        TopKWords.countWordsInFixedWindows(input, windowDuration, 3);
    PAssert.that(output)
        .containsInAnyOrder(
            KV.of("line", 3L),
            KV.of("first", 3L),
            KV.of("the", 3L),
            KV.of("line", 3L),
            KV.of("in", 2L),
            KV.of("window", 2L));
    pipeline.run();
  }

  private TestStream<String> createInput(Instant startOfTenSecondWindow) {
    return TestStream.create(StringUtf8Coder.of())
        .addElements(
            TimestampedValue.of("This is the first line.", startOfTenSecondWindow),
            TimestampedValue.of(
                "This is second line in the first window", startOfTenSecondWindow.plus(1000)),
            TimestampedValue.of("Last line in the first window", startOfTenSecondWindow.plus(2000)),
            TimestampedValue.of(
                "This is another line, but in different window.",
                startOfTenSecondWindow.plus(10000)),
            TimestampedValue.of(
                "Last line, in the same window as previous line.",
                startOfTenSecondWindow.plus(11000)))
        .advanceWatermarkToInfinity();
  }
}
