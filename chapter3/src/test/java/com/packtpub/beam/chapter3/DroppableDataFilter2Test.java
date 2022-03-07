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

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Iterables;
import java.util.Random;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;

public class DroppableDataFilter2Test {

  private final Random random = new Random();

  @Test
  public void testDroppableData() {
    Instant rawNow = Instant.now();
    // now aligned to one minute boundary
    Instant now = rawNow.minus(rawNow.getMillis() % 60000);
    TestStream<String> stream =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(now)
            .addElements(ts("a", now.plus(1))) // fine, before watermark - on time
            .advanceWatermarkTo(now.plus(1000))
            .advanceWatermarkTo(now.plus(1999))
            .addElements(ts("b", now.plus(999))) // late, but within allowed lateness
            .addElements(ts("c", now.plus(500))) // late, but within allowed lateness
            .advanceWatermarkTo(now.plus(5000))
            .addElements(ts("d", now)) // droppable
            .advanceWatermarkToInfinity();
    Pipeline p = Pipeline.create();
    PCollection<String> input =
        p.apply(stream)
            .apply(
                Window.<String>into(FixedWindows.of(Duration.standardSeconds(1)))
                    .withAllowedLateness(Duration.standardSeconds(1))
                    .discardingFiredPanes());
    PCollectionTuple split =
        DroppableDataFilter2.splitDroppable(
            DroppableDataFilter2.mainOutput, DroppableDataFilter2.droppableOutput, input);
    PAssert.that(split.get(DroppableDataFilter2.mainOutput))
        .inWindow(new IntervalWindow(now, now.plus(1000)))
        .containsInAnyOrder("a", "b", "c");
    PAssert.that(split.get(DroppableDataFilter2.droppableOutput))
        .inWindow(GlobalWindow.INSTANCE)
        .containsInAnyOrder("d");
    p.run().waitUntilFinish();
  }

  @Test
  public void testContract() {
    int numElements = 100;
    Instant now = Instant.now();
    TestStream.Builder<String> builder = TestStream.create(StringUtf8Coder.of());
    for (int i = 0; i < numElements; i++) {
      Instant currentNow = now.plus(100 * i);
      TimestampedValue<String> value = newValue(currentNow, i);
      builder = builder.addElements(value).advanceWatermarkTo(currentNow);
    }
    Pipeline p = Pipeline.create();
    PCollection<String> input =
        p.apply(builder.advanceWatermarkToInfinity())
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))));

    PCollectionTuple result =
        DroppableDataFilter2.splitDroppable(
            DroppableDataFilter2.mainOutput, DroppableDataFilter2.droppableOutput, input);

    PCollection<Long> mainCounted =
        result
            .get(DroppableDataFilter2.mainOutput)
            .apply(Combine.globally(Count.<String>combineFn()).withoutDefaults())
            .apply(
                Window.<Long>into(new GlobalWindows())
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .accumulatingFiredPanes()
                    .withAllowedLateness(Duration.ZERO))
            .apply(Sum.longsGlobally());
    PCollection<Long> droppableCounted =
        result
            .get(DroppableDataFilter2.droppableOutput)
            .apply(
                Window.<String>into(new GlobalWindows())
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .accumulatingFiredPanes()
                    .withAllowedLateness(Duration.ZERO))
            .apply(Count.globally());

    PCollection<Long> allElements =
        PCollectionList.of(mainCounted)
            .and(droppableCounted)
            .apply(Flatten.pCollections())
            .apply(Sum.longsGlobally());

    PAssert.that(allElements).containsInAnyOrder((long) numElements);
    PAssert.that(droppableCounted)
        .satisfies(
            e -> {
              assertTrue(Iterables.size(e) > 0);
              return null;
            });
    p.run();
  }

  private TimestampedValue<String> newValue(Instant now, int pos) {
    if (random.nextBoolean()) {
      Duration lateDuration = Duration.standardSeconds(random.nextInt(1800) + 60);
      return TimestampedValue.of(String.valueOf(pos), now.minus(lateDuration));
    }
    return TimestampedValue.of(String.valueOf(pos), now);
  }

  private TimestampedValue<String> ts(String s, Instant stamp) {
    return TimestampedValue.of(s, stamp);
  }
}
