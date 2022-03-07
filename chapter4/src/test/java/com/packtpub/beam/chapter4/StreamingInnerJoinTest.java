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

import java.util.function.Function;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;

public class StreamingInnerJoinTest {

  @Test
  public void testSimpleInnerJoin() {
    Pipeline p = Pipeline.create();
    PCollection<KV<String, String>> left =
        p.apply(Create.of(KV.of("alice", "F"), KV.of("bob", "M")));
    PCollection<KV<String, Double>> right =
        p.apply(Create.of(KV.of("F", 163.0), KV.of("M", 176.5)));
    TupleTag<KV<String, String>> leftTag = new TupleTag<>();
    TupleTag<KV<String, Double>> rightTag = new TupleTag<>();
    PCollection<String> result =
        PCollectionTuple.of(leftTag, left)
            .and(rightTag, right)
            .apply(
                new StreamingInnerJoin<>(
                    leftTag,
                    rightTag,
                    KV::getKey,
                    KV::getKey,
                    KV::getValue,
                    KV::getKey,
                    StringUtf8Coder.of(),
                    StringUtf8Coder.of()))
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(StreamingInnerJoinTest::formatOutput));

    PAssert.that(result).containsInAnyOrder("+alice:F:163.0", "+bob:M:176.5");
    p.run();
  }

  @Test
  public void testJoinWithChangedJoinKey() {
    Pipeline p = Pipeline.create();
    Instant now = new Instant(0);
    PCollection<KV<String, String>> left =
        p.apply(
            TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                .addElements(
                    TimestampedValue.of(KV.of("alice", "CA"), now),
                    TimestampedValue.of(KV.of("bob", "NY"), now))
                .advanceWatermarkTo(now.plus(Duration.standardDays(365)))
                .addElements(
                    TimestampedValue.of(
                        KV.of("alice", "NY"), now.plus(Duration.standardDays(2 * 365))))
                .advanceWatermarkToInfinity());
    PCollection<KV<String, Boolean>> right =
        p.apply(Create.of(KV.of("NY", true), KV.of("CA", false)));
    TupleTag<KV<String, String>> leftTag = new TupleTag<>();
    TupleTag<KV<String, Boolean>> rightTag = new TupleTag<>();
    PCollection<TimestampedValue<String>> result =
        PCollectionTuple.of(leftTag, left)
            .and(rightTag, right)
            .apply(
                new StreamingInnerJoin<>(
                    leftTag,
                    rightTag,
                    KV::getKey,
                    KV::getKey,
                    KV::getValue,
                    KV::getKey,
                    StringUtf8Coder.of(),
                    StringUtf8Coder.of()))
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(StreamingInnerJoinTest::formatOutput))
            .apply(Reify.timestamps());

    PAssert.that(result)
        .containsInAnyOrder(
            TimestampedValue.of("+alice:CA:false", now),
            TimestampedValue.of("+bob:NY:true", now),
            TimestampedValue.of("+alice:NY:true", now.plus(Duration.standardDays(2 * 365))),
            TimestampedValue.of("-alice:CA:false", now.plus(Duration.standardDays(2 * 365))));
    p.run();
  }

  @Test
  public void testJoinWithChangedRightValue() {
    Pipeline p = Pipeline.create();
    Instant now = new Instant(0);
    PCollection<KV<String, String>> left =
        p.apply(Create.of(KV.of("alice", "CA"), KV.of("bob", "CA")));
    PCollection<KV<String, Boolean>> right =
        p.apply(
            TestStream.create(KvCoder.of(StringUtf8Coder.of(), BooleanCoder.of()))
                .addElements(
                    TimestampedValue.of(KV.of("CA", true), now),
                    TimestampedValue.of(KV.of("CA", false), now.plus(Duration.standardDays(1))))
                .advanceWatermarkToInfinity());
    TupleTag<KV<String, String>> leftTag = new TupleTag<>();
    TupleTag<KV<String, Boolean>> rightTag = new TupleTag<>();
    PCollection<TimestampedValue<String>> result =
        PCollectionTuple.of(leftTag, left)
            .and(rightTag, right)
            .apply(
                new StreamingInnerJoin<>(
                    leftTag,
                    rightTag,
                    KV::getKey,
                    KV::getKey,
                    KV::getValue,
                    KV::getKey,
                    StringUtf8Coder.of(),
                    StringUtf8Coder.of()))
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(StreamingInnerJoinTest::formatOutput))
            .apply(Reify.timestamps());

    PAssert.that(result)
        .containsInAnyOrder(
            TimestampedValue.of("+alice:CA:true", now),
            TimestampedValue.of("+bob:CA:true", now),
            TimestampedValue.of("-alice:CA:true", now.plus(Duration.standardDays(1))),
            TimestampedValue.of("-bob:CA:true", now.plus(Duration.standardDays(1))),
            TimestampedValue.of("+alice:CA:false", now.plus(Duration.standardDays(1))),
            TimestampedValue.of("+bob:CA:false", now.plus(Duration.standardDays(1))));
    p.run();
  }

  @Test
  public void testJoinWithMultiValuesLeftAndRight() {
    Pipeline p = Pipeline.create();
    Instant now = new Instant(0);
    PCollection<KV<String, String>> left =
        p.apply(
            Create.timestamped(
                TimestampedValue.of(KV.of("alice", "CA"), now),
                TimestampedValue.of(KV.of("bob", "CA"), now.plus(Duration.standardDays(1)))));
    PCollection<KV<String, String>> right =
        p.apply(
            Create.timestamped(
                TimestampedValue.of(KV.of("CA.1", "Los Angeles"), now),
                TimestampedValue.of(
                    KV.of("CA.2", "San Francisco"), now.plus(Duration.standardDays(1))),
                TimestampedValue.of(KV.of("CA.1", ""), now.plus(Duration.standardDays(2)))));
    TupleTag<KV<String, String>> leftTag = new TupleTag<>();
    TupleTag<KV<String, String>> rightTag = new TupleTag<>();
    PCollectionTuple joinInput = PCollectionTuple.of(leftTag, left).and(rightTag, right);
    PCollection<TimestampedValue<String>> result =
        joinInput
            .apply(
                new StreamingInnerJoin<>(
                    leftTag,
                    rightTag,
                    KV::getKey,
                    KV::getKey,
                    KV::getValue,
                    r -> r.getKey().substring(0, 2),
                    StringUtf8Coder.of(),
                    StringUtf8Coder.of()))
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(StreamingInnerJoinTest::formatOutput))
            .apply(Reify.timestamps());

    PAssert.that(result)
        .containsInAnyOrder(
            TimestampedValue.of("+alice:CA:Los Angeles", now),
            TimestampedValue.of("+bob:CA:Los Angeles", now.plus(Duration.standardDays(1))),
            TimestampedValue.of("+alice:CA:San Francisco", now.plus(Duration.standardDays(1))),
            TimestampedValue.of("+bob:CA:San Francisco", now.plus(Duration.standardDays(1))),
            TimestampedValue.of("-alice:CA:Los Angeles", now.plus(Duration.standardDays(2))),
            TimestampedValue.of("-bob:CA:Los Angeles", now.plus(Duration.standardDays(2))),
            TimestampedValue.of("+alice:CA:", now.plus(Duration.standardDays(2))),
            TimestampedValue.of("+bob:CA:", now.plus(Duration.standardDays(2))));
    p.run();
  }

  private static <K, J, L extends KV<?, ?>, R extends KV<?, ?>> String formatOutput(
      JoinResult<K, J, L, R> value) {
    return formatOutput(value, KV::getValue, KV::getValue);
  }

  private static <K, J, L extends KV<?, ?>, R extends KV<?, ?>> String formatOutput(
      JoinResult<K, J, L, R> value,
      Function<L, ?> leftValueExtractor,
      Function<R, ?> rightValueExtractor) {

    return (value.isRetract() ? "-" : "+")
        + value.getLeftPrimaryKey()
        + ":"
        + leftValueExtractor.apply(value.getLeftValue())
        + ":"
        + rightValueExtractor.apply(value.getRightValue());
  }
}
