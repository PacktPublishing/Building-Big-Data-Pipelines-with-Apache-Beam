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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

class ComputeAverage
    extends PTransform<PCollection<KV<String, Metric>>, PCollection<KV<String, Double>>> {

  @Override
  public PCollection<KV<String, Double>> expand(PCollection<KV<String, Metric>> input) {
    return input.apply(Combine.perKey(new AveragePaceFn()));
  }

  private static class AveragePaceFn extends CombineFn<Metric, MetricAccumulator, Double> {

    @Override
    public MetricAccumulator createAccumulator() {
      return new MetricAccumulator();
    }

    @Override
    public Coder<MetricAccumulator> getAccumulatorCoder(
        CoderRegistry registry, Coder<Metric> inputCoder) {
      return new MetricAccumulator.Coder();
    }

    @Override
    public MetricAccumulator addInput(MetricAccumulator accumulator, Metric input) {
      return accumulator.add(input);
    }

    @Override
    public MetricAccumulator mergeAccumulators(Iterable<MetricAccumulator> accumulators) {
      MetricAccumulator merged = null;
      for (MetricAccumulator acc : accumulators) {
        if (merged == null) {
          merged = acc;
        } else {
          merged = merged.merge(acc);
        }
      }
      return merged;
    }

    @Override
    public Double extractOutput(MetricAccumulator accumulator) {
      return accumulator.output();
    }
  }

  private static class MetricAccumulator {
    long totalDuration;
    double totalDistance;

    MetricAccumulator() {
      this(0, 0.0);
    }

    public MetricAccumulator(long totalDuration, double totalDistance) {
      this.totalDuration = totalDuration;
      this.totalDistance = totalDistance;
    }

    public MetricAccumulator add(Metric input) {
      totalDistance += input.getLength();
      totalDuration += input.getDuration();
      return this;
    }

    public Double output() {
      return (totalDistance * 1000) / totalDuration;
    }

    public MetricAccumulator merge(MetricAccumulator acc) {
      return new MetricAccumulator(
          totalDuration + acc.totalDuration, totalDistance + acc.totalDistance);
    }

    public static class Coder extends CustomCoder<MetricAccumulator> {

      private static final DoubleCoder DOUBLE_CODER = DoubleCoder.of();
      private static final VarLongCoder LONG_CODER = VarLongCoder.of();

      @Override
      public void encode(MetricAccumulator value, OutputStream outStream)
          throws CoderException, IOException {

        LONG_CODER.encode(value.totalDuration, outStream);
        DOUBLE_CODER.encode(value.totalDistance, outStream);
      }

      @Override
      public MetricAccumulator decode(InputStream inStream) throws CoderException, IOException {
        return new MetricAccumulator(LONG_CODER.decode(inStream), DOUBLE_CODER.decode(inStream));
      }
    }
  }
}
