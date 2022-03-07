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

import com.packtpub.beam.util.ToMetric.Metric;
import com.packtpub.beam.util.ToMetric.MetricCoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;

public class ComputeAverageTest {

  @Test
  public void testComputationBatch() {
    Pipeline p = Pipeline.create();
    Instant now = Instant.ofEpochMilli(1234567890000L);
    PCollection<KV<String, Metric>> input =
        p.apply(
            Create.timestamped(
                    TimestampedValue.of(KV.of("foo", new Metric(90, 30000)), now),
                    TimestampedValue.of(KV.of("bar", new Metric(30, 30000)), now),
                    TimestampedValue.of(KV.of("foo", new Metric(70, 10000)), now.plus(10000)),
                    TimestampedValue.of(KV.of("bar", new Metric(180, 60000)), now.plus(89999)))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), new MetricCoder())));
    PCollection<KV<String, Double>> result =
        input
            .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
            .apply(new ComputeAverage());
    PAssert.that(result)
        .containsInAnyOrder(KV.of("foo", 4.0), KV.of("bar", 1.0), KV.of("bar", 3.0));
    p.run().waitUntilFinish();
  }
}
