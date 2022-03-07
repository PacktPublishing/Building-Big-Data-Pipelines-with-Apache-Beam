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
package com.packtpub.beam.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.packtpub.beam.util.ToMetric.Metric;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;

public class ToMetricTest {

  @Test
  public void testComputationBatch() {
    Pipeline p = Pipeline.create();
    Instant now = Instant.ofEpochMilli(1234567890000L);
    Position initial = Position.random(now.getMillis());
    PCollection<KV<String, Position>> input =
        p.apply(
            Create.timestamped(
                    TimestampedValue.of(KV.of("foo", initial), now),
                    TimestampedValue.of(
                        KV.of("foo", initial.move(1, 1, 3, 120000)), now.plus(120000)))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), new PositionCoder())));
    PCollection<TimestampedValue<KV<String, Metric>>> result =
        input.apply(new ToMetric()).apply(Reify.timestamps());
    PAssert.that(result)
        .satisfies(
            values -> {
              for (TimestampedValue<KV<String, Metric>> val : values) {
                if (val.getTimestamp().equals(now.plus(30000))) {
                  assertEquals("foo", val.getValue().getKey());
                  assertEquals(90, val.getValue().getValue().getLength(), 0.0001);
                  assertEquals(30000, val.getValue().getValue().getDuration());
                } else if (val.getTimestamp().equals(now.plus(90000))) {
                  assertEquals("foo", val.getValue().getKey());
                  assertEquals(180, val.getValue().getValue().getLength(), 0.0001);
                  assertEquals(60000, val.getValue().getValue().getDuration());
                } else if (val.getTimestamp().equals(now.plus(120000))) {
                  assertEquals("foo", val.getValue().getKey());
                  assertEquals(90, val.getValue().getValue().getLength(), 0.0001);
                  assertEquals(30000, val.getValue().getValue().getDuration());
                } else {
                  fail("Unrecognized stamp " + val.getTimestamp() + ", base " + now);
                }
              }
              return null;
            });
    p.run().waitUntilFinish();
  }

  @Test
  public void testComputationStream() {
    Pipeline p = Pipeline.create();
    Instant now = Instant.ofEpochMilli(1234567890000L);
    Position initial = Position.random(now.getMillis());
    PCollection<KV<String, Position>> input =
        p.apply(
            TestStream.create(KvCoder.of(StringUtf8Coder.of(), new PositionCoder()))
                .addElements(TimestampedValue.of(KV.of("foo", initial), now))
                .advanceWatermarkTo(now)
                .addElements(
                    TimestampedValue.of(
                        KV.of("foo", initial.move(1, 1, 3, 120000)), now.plus(120000)))
                .advanceWatermarkTo(now.plus(300000))
                .advanceWatermarkToInfinity());
    PCollection<TimestampedValue<KV<String, Metric>>> result =
        input.apply(new ToMetric()).apply(Reify.timestamps());
    PAssert.that(result)
        .satisfies(
            values -> {
              for (TimestampedValue<KV<String, Metric>> val : values) {
                if (val.getTimestamp().equals(now.plus(30000))) {
                  assertEquals("foo", val.getValue().getKey());
                  assertEquals(90, val.getValue().getValue().getLength(), 0.0001);
                  assertEquals(30000, val.getValue().getValue().getDuration());
                } else if (val.getTimestamp().equals(now.plus(90000))) {
                  assertEquals("foo", val.getValue().getKey());
                  assertEquals(180, val.getValue().getValue().getLength(), 0.0001);
                  assertEquals(60000, val.getValue().getValue().getDuration());
                } else if (val.getTimestamp().equals(now.plus(120000))) {
                  assertEquals("foo", val.getValue().getKey());
                  assertEquals(90, val.getValue().getValue().getLength(), 0.0001);
                  assertEquals(30000, val.getValue().getValue().getDuration());
                } else {
                  fail("Unrecognized stamp " + val.getTimestamp() + ", base " + now);
                }
              }
              return null;
            });
    p.run().waitUntilFinish();
  }
}
