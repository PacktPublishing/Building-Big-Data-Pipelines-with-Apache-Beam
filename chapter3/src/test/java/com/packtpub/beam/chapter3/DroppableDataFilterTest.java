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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class DroppableDataFilterTest {

  @Test
  public void testDroppableData() {
    Instant rawNow = Instant.now();
    // now aligned to one minute boundary
    Instant now = rawNow.minus(rawNow.getMillis() % 60000);
    TestStream<String> stream =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(now)
            .addElements(ts("a", now.plus(1))) // fine, before watermark - on time
            .advanceWatermarkTo(now.plus(1999))
            .addElements(ts("b", now.plus(999))) // late, but within allowed lateness
            .advanceWatermarkTo(now.plus(2000))
            .addElements(ts("c", now)) // droppable
            .advanceWatermarkToInfinity();
    Pipeline p = Pipeline.create();
    PCollection<String> input =
        p.apply(stream)
            .apply(
                Window.<String>into(FixedWindows.of(Duration.standardSeconds(1)))
                    .withAllowedLateness(Duration.standardSeconds(1))
                    .discardingFiredPanes());
    PCollectionTuple split =
        DroppableDataFilter.splitDroppable(
            DroppableDataFilter.mainOutput, DroppableDataFilter.droppableOutput, input);
    PAssert.that(split.get(DroppableDataFilter.mainOutput))
        .inWindow(new IntervalWindow(now, now.plus(1000)))
        .containsInAnyOrder("a", "b");
    PAssert.that(split.get(DroppableDataFilter.droppableOutput))
        .inWindow(GlobalWindow.INSTANCE)
        .containsInAnyOrder("c");
    p.run().waitUntilFinish();
  }

  @Test
  @Disabled("This would fail, we need a complex fix for this, see DroppableDataFilter2")
  public void testDroppableDataFirst() {
    Instant rawNow = Instant.now();
    // now aligned to one minute boundary
    Instant now = rawNow.minus(rawNow.getMillis() % 60000);
    TestStream<String> stream =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(now.plus(2000))
            .addElements(ts("1", now)) // droppable
            .advanceWatermarkToInfinity();
    Pipeline p = Pipeline.create();
    PCollection<String> input =
        p.apply(stream)
            .apply(
                Window.<String>into(FixedWindows.of(Duration.standardSeconds(1)))
                    .withAllowedLateness(Duration.standardSeconds(1))
                    .discardingFiredPanes());
    PCollectionTuple split =
        DroppableDataFilter.splitDroppable(
            DroppableDataFilter.mainOutput, DroppableDataFilter.droppableOutput, input);
    PAssert.that(split.get(DroppableDataFilter.droppableOutput))
        .inWindow(GlobalWindow.INSTANCE)
        .containsInAnyOrder("1");
    p.run().waitUntilFinish();
  }

  private TimestampedValue<String> ts(String s, Instant stamp) {
    return TimestampedValue.of(s, stamp);
  }
}
