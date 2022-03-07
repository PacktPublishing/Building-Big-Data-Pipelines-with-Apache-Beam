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

import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Consumer;
import lombok.Value;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class ToMetric
    extends PTransform<
        PCollection<KV<String, Position>>, PCollection<KV<String, ToMetric.Metric>>> {

  @Value
  public static class Metric {
    double length;
    long duration;

    public Metric(double length, long duration) {
      this.length = length;
      this.duration = duration;
    }
  }

  public static class MetricCoder extends CustomCoder<Metric> {

    private static final DoubleCoder DOUBLE_CODER = DoubleCoder.of();
    private static final VarLongCoder LONG_CODER = VarLongCoder.of();

    @Override
    public void encode(Metric value, OutputStream outStream) throws CoderException, IOException {
      DOUBLE_CODER.encode(value.getLength(), outStream);
      LONG_CODER.encode(value.getDuration(), outStream);
    }

    @Override
    public Metric decode(InputStream inStream) throws CoderException, IOException {
      return new Metric(DOUBLE_CODER.decode(inStream), LONG_CODER.decode(inStream));
    }
  }

  @Override
  public PCollection<KV<String, Metric>> expand(PCollection<KV<String, Position>> input) {
    return input
        .apply("globalWindow", Window.into(new GlobalWindows()))
        .apply("generateInMinuteIntervals", ParDo.of(new ReportInMinuteIntervalsFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), new MetricCoder()));
  }

  /** Generate an estimation of position in beginning and end of each 1 minute interval. */
  private static class ReportInMinuteIntervalsFn
      extends DoFn<KV<String, Position>, KV<String, Metric>> {

    @TimerId("flushPosition")
    private final TimerSpec flushPositionTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @StateId("minTimerOutputTs")
    private final StateSpec<ValueState<Instant>> minTimerOutputTs = StateSpecs.value();

    @StateId("positions")
    private final StateSpec<BagState<Position>> cachedPositions =
        StateSpecs.bag(new PositionCoder());

    @ProcessElement
    public void processElement(
        @Element KV<String, Position> element,
        @Timestamp Instant ts,
        @StateId("positions") BagState<Position> positionsState,
        @StateId("minTimerOutputTs") ValueState<Instant> minTimerOutputTs,
        @TimerId("flushPosition") Timer flushTimer) {

      Instant minTimerTs =
          MoreObjects.firstNonNull(minTimerOutputTs.read(), BoundedWindow.TIMESTAMP_MAX_VALUE);
      if (ts.isBefore(minTimerTs)) {
        flushTimer.withOutputTimestamp(ts).set(ts);
        minTimerOutputTs.write(ts);
      }
      positionsState.add(element.getValue());
    }

    @OnTimer("flushPosition")
    public void onFlushTimer(
        OnTimerContext context,
        @Key String key,
        @StateId("positions") BagState<Position> positions,
        @StateId("minTimerOutputTs") ValueState<Instant> minTimerOutputTs,
        @TimerId("flushPosition") Timer timer,
        OutputReceiver<KV<String, Metric>> output) {

      minTimerOutputTs.write(BoundedWindow.TIMESTAMP_MIN_VALUE);
      PriorityQueue<Position> queue =
          new PriorityQueue<>(Comparator.comparing(Position::getTimestamp));
      Instant ts = context.fireTimestamp();

      List<Position> keep = new ArrayList<>();
      long minTsToKeep = BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
      for (Position pos : positions.read()) {
        if (pos.getTimestamp() < ts.getMillis()) {
          queue.add(pos);
        } else {
          if (minTsToKeep > pos.getTimestamp()) {
            minTsToKeep = pos.getTimestamp();
          }
          keep.add(pos);
        }
      }
      if (!queue.isEmpty()) {
        Position first = queue.poll();
        Position last =
            computeMinuteMetrics(
                first,
                queue,
                p -> output.outputWithTimestamp(KV.of(key, p.getValue()), p.getTimestamp()));
        keep.add(last);
        minTsToKeep = last.getTimestamp();
      }
      positions.clear();
      keep.forEach(positions::add);
      setNewLoopTimer(timer, ts, minTsToKeep);
    }

    private void setNewLoopTimer(Timer timer, Instant currentStamp, long minTsToKeep) {
      if (currentStamp.getMillis() > minTsToKeep + 300000) {
        minTsToKeep = currentStamp.getMillis();
      }
      if (minTsToKeep < BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
        timer
            .withOutputTimestamp(Instant.ofEpochMilli(minTsToKeep))
            .offset(Duration.ZERO)
            .align(Duration.standardMinutes(1))
            .setRelative();
      } else {
        timer.offset(Duration.ZERO).align(Duration.standardMinutes(1)).setRelative();
      }
    }

    // compute metrics with minute alignment and return last known position
    private Position computeMinuteMetrics(
        Position first,
        PriorityQueue<Position> queue,
        Consumer<TimestampedValue<Metric>> outputConsumer) {

      Position current = first;
      while (!queue.isEmpty()) {
        Position pos = queue.poll();
        long timeDiffMs = pos.getTimestamp() - current.getTimestamp();
        if (timeDiffMs > 0) {
          double latitudeDiff = pos.getLatitude() - current.getLatitude();
          double longitudeDiff = pos.getLongitude() - current.getLongitude();
          double distance = pos.distance(current);
          double avgSpeedMeterPerSec = (distance * 1000) / timeDiffMs;
          while (current.getTimestamp() < pos.getTimestamp()) {
            // compute time difference to end of 1 minute boundary
            long deltaMs =
                Math.min(
                    pos.getTimestamp() - current.getTimestamp(),
                    60000 - current.getTimestamp() % 60000);
            Position nextPos =
                current.move(latitudeDiff, longitudeDiff, avgSpeedMeterPerSec, deltaMs);
            Metric metric = new Metric(nextPos.distance(current), deltaMs);
            outputConsumer.accept(
                TimestampedValue.of(metric, Instant.ofEpochMilli(nextPos.getTimestamp())));
            current = nextPos;
          }
        }
      }
      return current;
    }
  }
}
