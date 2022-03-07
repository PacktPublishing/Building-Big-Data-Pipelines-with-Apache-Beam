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
package com.packtpub.beam.chapter4;

import com.packtpub.beam.util.Position;
import com.packtpub.beam.util.PositionCoder;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;

public class SportTrackerMotivationUsingJoinLibraryTest {
  private final Instant now = Instant.ofEpochMilli(1234567890000L);

  @Test
  public void testComputationBatch() {
    AtomicReference<Position> fooPos = new AtomicReference<>(Position.random(now.getMillis()));
    AtomicReference<Position> barPos = new AtomicReference<>(Position.random(now.getMillis()));
    Pipeline p = Pipeline.create();
    PCollection<KV<String, Position>> input =
        p.apply(
            Create.timestamped(
                    TimestampedValue.of(
                        KV.of("foo", fooPos.getAndUpdate(old -> old.move(1, 1, 3, 180000))), now),
                    TimestampedValue.of(
                        KV.of("bar", barPos.getAndUpdate(old -> old.move(1, 1, 2, 240000))), now),
                    TimestampedValue.of(
                        KV.of("foo", fooPos.getAndUpdate(old -> old.move(1, 1, 2.5, 180000))), now),
                    TimestampedValue.of(
                        KV.of("bar", barPos.getAndUpdate(old -> old.move(1, 1, 2.5, 120000))), now),
                    TimestampedValue.of(KV.of("foo", fooPos.get()), now.plus(360000)),
                    TimestampedValue.of(KV.of("bar", barPos.get()), now.plus(360000)))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), new PositionCoder())));

    PCollection<KV<String, Boolean>> result =
        input.apply(new SportTrackerMotivationUsingJoinLibrary());
    PAssert.that(result)
        .containsInAnyOrder(
            KV.of("foo", false), KV.of("bar", true), KV.of("foo", false), KV.of("bar", true));
    p.run();
  }

  @Test
  public void testComputationStream() {
    AtomicReference<Position> fooPos = new AtomicReference<>(Position.random(now.getMillis()));
    Pipeline p = Pipeline.create();
    PCollection<KV<String, Position>> input =
        p.apply(
            TestStream.create(KvCoder.of(StringUtf8Coder.of(), new PositionCoder()))
                .addElements(
                    // move for 5 minutes
                    TimestampedValue.of(
                        KV.of("foo", fooPos.getAndUpdate(old -> old.move(1, 1, 3, 300000))), now),
                    // and then 30 seconds at higher pace - our seed time begins in the
                    // middle of a minute
                    TimestampedValue.of(
                        KV.of("foo", fooPos.getAndUpdate(old -> old.move(1, 1, 3.5, 30000))),
                        now.plus(300000)))
                .advanceWatermarkTo(now.plus(300000))
                .addElements(TimestampedValue.of(KV.of("foo", fooPos.get()), now.plus(330000)))
                .advanceWatermarkToInfinity());

    PCollection<KV<String, Boolean>> result =
        input.apply(new SportTrackerMotivationUsingJoinLibrary());
    PAssert.that(result).containsInAnyOrder(KV.of("foo", true));
    p.run();
  }
}
