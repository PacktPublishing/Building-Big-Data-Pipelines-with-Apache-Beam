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
package com.packtpub.beam.chapter7;

import com.google.common.annotations.VisibleForTesting;
import com.packtpub.beam.util.PrintElements;
import java.io.Serializable;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class PiSampler extends PTransform<PBegin, PCollection<Double>> {

  public interface RandomFactory extends Serializable {
    Random apply();
  }

  private static void usage() {
    System.err.printf(
        "Usage: %s <num samples> <parallelism> [<other args>]%n", PiSampler.class.getSimpleName());
    System.exit(1);
  }

  public static PiSampler of(long numSamples, long parallelism) {
    return new PiSampler(numSamples, parallelism);
  }

  public static void main(String[] args) {
    if (args.length < 2) {
      usage();
    }
    long numSamples = Long.parseLong(args[0]);
    long parallelism = Long.parseLong(args[1]);
    Pipeline p =
        Pipeline.create(
            PipelineOptionsFactory.fromArgs(Arrays.copyOfRange(args, 2, args.length)).create());
    PCollection<Double> result = p.apply(PiSampler.of(numSamples, parallelism));
    result.apply(PrintElements.of());
    p.run().waitUntilFinish();
  }

  private final long numSamples;
  private final long parallelism;
  private final RandomFactory randomFactory;

  PiSampler(long numSamples, long parallelism) {
    this.numSamples = numSamples;
    this.parallelism = parallelism;
    this.randomFactory = this::newRandom;
  }

  PiSampler(PiSampler other, RandomFactory randomFactory) {
    this.numSamples = other.numSamples;
    this.parallelism = other.parallelism;
    this.randomFactory = randomFactory;
  }

  @VisibleForTesting
  PiSampler withRandomFactory(RandomFactory randomFactory) {
    return new PiSampler(this, randomFactory);
  }

  private Random newRandom() {
    return new Random(new SecureRandom().nextLong());
  }

  @Override
  public PCollection<Double> expand(PBegin input) {
    return input
        .apply(Impulse.create())
        .apply(
            FlatMapElements.into(TypeDescriptors.strings())
                .via(
                    e ->
                        LongStream.range(0, parallelism)
                            .mapToObj(i -> "")
                            .collect(Collectors.toList())))
        .apply(Reshuffle.viaRandomKey())
        .apply(ParDo.of(new SampleDoFn()))
        .apply(Sum.longsGlobally())
        .apply(
            MapElements.into(TypeDescriptors.doubles())
                .via(c -> 4 * (1.0 - c / (double) (numSamples * parallelism))));
  }

  private class SampleDoFn extends DoFn<String, Long> {

    @ProcessElement
    public void process(
        RestrictionTracker<OffsetRange, Long> tracker, OutputReceiver<Long> output) {

      final Random random = randomFactory.apply();

      long off = tracker.currentRestriction().getFrom();
      while (tracker.tryClaim(off++)) {
        double x = random.nextDouble();
        double y = random.nextDouble();
        if (x * x + y * y > 1) {
          output.output(1L);
        }
      }
    }

    @GetInitialRestriction
    public OffsetRange initialRestriction() {
      return new OffsetRange(0, numSamples);
    }

    @GetRestrictionCoder
    public OffsetRange.Coder getRestrictionCoder() {
      return OffsetRange.Coder.of();
    }
  }
}
