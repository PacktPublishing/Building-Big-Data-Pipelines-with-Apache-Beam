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

import static com.packtpub.beam.util.Position.EARTH_DIAMETER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.packtpub.beam.util.Position;
import com.packtpub.beam.util.PositionCoder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.TestStream.Builder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;

public class SchemaSportTrackerTest {

  @Test
  public void testGpsDeltaLengthCalculation() {
    long now = System.currentTimeMillis();
    Position first = new Position(64.53165930148285, -17.3172239914558, now);
    Position second = new Position(64.53168645625584, -17.316554613536688, now);
    double distance = first.distance(second);
    assertTrue(distance < 80, "Expected less than 80 meters, got " + distance);
    assertTrue(distance > 30, "Expected more than 30 meters, got " + distance);
    assertEquals(distance, second.distance(first), 0.001);
    assertEquals(0.0, first.distance(first), 0.0001);
    assertEquals(0.0, second.distance(second), 0.0001);
    assertEquals(
        2 * EARTH_DIAMETER,
        new Position(90.0, 0.0, now).distance(new Position(-90.0, 0.0, now)),
        0.01);
    assertEquals(
        2 * EARTH_DIAMETER,
        new Position(0.0, 90.0, now).distance(new Position(0.0, -90.0, now)),
        0.01);
    assertEquals(
        Math.sqrt(2) * EARTH_DIAMETER,
        new Position(90.0, 0.0, now).distance(new Position(0.0, 00.0, now)),
        0.01);
    double circumference = 2.0 * Math.PI * EARTH_DIAMETER;
    double angle100 = 100.0 / circumference * 360;
    Position zero = new Position(0, 0, now);
    assertEquals(100.0, zero.distance(new Position(0, angle100, now)), 0.0001);
  }

  @Test
  public void testGpsDeltaLengthCalculation2() {
    long now = System.currentTimeMillis();
    Position first = new Position(0, 0, now);
    Position second = new Position(0, 0.1, now);
    assertEquals(11119, first.distance(second), 1);
    assertEquals(11119, second.distance(first), 1);
    second = new Position(0.001, 0.001, now);
    assertEquals(Math.sqrt(2) * 111, first.distance(second), 1);
  }

  @Test
  public void testPipeline() throws IOException {

    Pipeline pipeline = Pipeline.create();
    SchemaSportTracker.registerCoders(pipeline);
    List<KV<String, Position>> input =
        loadDataFromStream(
            getClass().getClassLoader().getResourceAsStream("test-tracker-data.txt"));

    PCollection<String> result =
        SchemaSportTracker.computeTrackerMetrics(pipeline.apply(asTestStream(input)))
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        r ->
                            String.format(
                                "%s:%d,%d",
                                r.getRow(0).getString(0),
                                Math.round(r.getRow(1).getRow(0).getDouble(0)),
                                r.getRow(1).getRow(0).getInt64(1))));

    PAssert.that(result)
        .inOnTimePane(GlobalWindow.INSTANCE)
        .containsInAnyOrder("track1:614,257", "track2:5641,1262");

    pipeline.run();
  }

  private TestStream<KV<String, Position>> asTestStream(List<KV<String, Position>> input) {
    Builder<KV<String, Position>> builder =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), new PositionCoder()));
    for (KV<String, Position> kv : input) {
      builder =
          builder.addElements(
              TimestampedValue.of(kv, Instant.ofEpochMilli(kv.getValue().getTimestamp())));
    }
    return builder.advanceWatermarkToInfinity();
  }

  private List<KV<String, Position>> loadDataFromStream(InputStream stream) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
      return reader
          .lines()
          .map(l -> l.split("\t"))
          .filter(p -> p.length == 4)
          .map(
              parts ->
                  KV.of(
                      parts[0],
                      new Position(
                          Double.parseDouble(parts[1]),
                          Double.parseDouble(parts[2]),
                          Long.parseLong(parts[3]))))
          .collect(Collectors.toList());
    }
  }
}
