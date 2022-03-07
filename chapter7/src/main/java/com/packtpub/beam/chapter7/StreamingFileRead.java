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
import com.google.common.base.Preconditions;
import com.packtpub.beam.util.PrintElements;
import com.packtpub.beam.util.Tokenize;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class StreamingFileRead extends PTransform<PBegin, PCollection<String>> {

  private static void usage() {
    System.err.printf(
        "Usage: %s <directory_path> [<other args>]%n", StreamingFileRead.class.getSimpleName());
    System.exit(1);
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      usage();
    }
    String path = args[0];
    Pipeline p =
        Pipeline.create(
            PipelineOptionsFactory.fromArgs(Arrays.copyOfRange(args, 1, args.length)).create());
    p.apply(StreamingFileRead.of(path))
        .apply(Tokenize.of())
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))))
        .apply(Count.perElement())
        .apply(PrintElements.of());
    p.run();
  }

  public static StreamingFileRead of(String directoryPath) {
    return new StreamingFileRead(directoryPath);
  }

  @VisibleForTesting
  static class DirectoryWatch extends PTransform<PCollection<String>, PCollection<String>> {

    private SerializableFunction<KV<String, Instant>, Instant> watermarkFn = KV::getValue;

    DirectoryWatch withWatermarkFn(SerializableFunction<KV<String, Instant>, Instant> watermarkFn) {
      this.watermarkFn = watermarkFn;
      return this;
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
      return input.apply(ParDo.of(new DirectoryWatchFn(watermarkFn)));
    }
  }

  @VisibleForTesting
  static class FileRead extends PTransform<PCollection<String>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<String> input) {
      return input.apply(ParDo.of(new FileReadFn()));
    }
  }

  @AllArgsConstructor
  private static class DirectoryWatchRestriction
      implements HasDefaultTracker<DirectoryWatchRestriction, DirectoryWatchRestrictionTracker> {

    public static class Coder extends CustomCoder<DirectoryWatchRestriction> {

      private static final SetCoder<String> SET_STRING_CODER = SetCoder.of(StringUtf8Coder.of());
      private static final BooleanCoder BOOL_CODER = BooleanCoder.of();

      @Override
      public void encode(DirectoryWatchRestriction value, OutputStream outStream)
          throws CoderException, IOException {
        SET_STRING_CODER.encode(value.getAlreadyProcessed(), outStream);
        BOOL_CODER.encode(value.isFinished(), outStream);
      }

      @Override
      public DirectoryWatchRestriction decode(InputStream inStream)
          throws CoderException, IOException {
        Set<String> processed = SET_STRING_CODER.decode(inStream);
        boolean finished = BOOL_CODER.decode(inStream);
        return new DirectoryWatchRestriction(processed, finished);
      }
    }

    @Getter private final Set<String> alreadyProcessed;
    @Getter boolean finished;

    public DirectoryWatchRestriction asPrimary() {
      finished = true;
      return this;
    }

    public DirectoryWatchRestriction asResidual() {
      return new DirectoryWatchRestriction(alreadyProcessed, false);
    }

    @Override
    public DirectoryWatchRestrictionTracker newTracker() {
      return new DirectoryWatchRestrictionTracker(this);
    }
  }

  private static class DirectoryWatchRestrictionTracker
      extends RestrictionTracker<DirectoryWatchRestriction, String> {

    private final DirectoryWatchRestriction restriction;

    DirectoryWatchRestrictionTracker(DirectoryWatchRestriction restriction) {
      this.restriction = restriction;
    }

    @Override
    public boolean tryClaim(String newFile) {
      if (restriction.isFinished()) {
        return false;
      }
      restriction.getAlreadyProcessed().add(newFile);
      return true;
    }

    @Override
    public DirectoryWatchRestriction currentRestriction() {
      return restriction;
    }

    @Override
    public SplitResult<DirectoryWatchRestriction> trySplit(double fractionOfRemainder) {
      return SplitResult.of(restriction.asPrimary(), restriction.asResidual());
    }

    @Override
    public void checkDone() throws IllegalStateException {}

    @Override
    public IsBounded isBounded() {
      return restriction.isFinished() ? IsBounded.BOUNDED : IsBounded.UNBOUNDED;
    }
  }

  private static class DirectoryWatchFn extends DoFn<String, String> {

    private final SerializableFunction<KV<String, Instant>, Instant> watermarkFn;

    public DirectoryWatchFn(SerializableFunction<KV<String, Instant>, Instant> watermarkFn) {
      this.watermarkFn = watermarkFn;
    }

    @ProcessElement
    public ProcessContinuation process(
        @Element String path,
        RestrictionTracker<DirectoryWatchRestriction, String> tracker,
        ManualWatermarkEstimator<Instant> watermarkEstimator,
        OutputReceiver<String> outputReceiver) {

      while (true) {
        List<KV<String, Instant>> newFiles = getNewFilesIfAny(path, tracker);
        if (newFiles.isEmpty()) {
          return ProcessContinuation.resume().withResumeDelay(Duration.millis(500));
        }
        if (!processNewFiles(tracker, watermarkEstimator, outputReceiver, newFiles)) {
          return ProcessContinuation.stop();
        }
      }
    }

    private boolean processNewFiles(
        RestrictionTracker<DirectoryWatchRestriction, String> tracker,
        ManualWatermarkEstimator<Instant> watermarkEstimator,
        OutputReceiver<String> outputReceiver,
        Iterable<KV<String, Instant>> newFiles) {

      Instant maxInstant = watermarkEstimator.currentWatermark();
      for (KV<String, Instant> newFile : newFiles) {
        if (!tracker.tryClaim(newFile.getKey())) {
          watermarkEstimator.setWatermark(maxInstant);
          return false;
        }
        outputReceiver.outputWithTimestamp(newFile.getKey(), newFile.getValue());
        Instant fileWatermark = watermarkFn.apply(newFile);
        if (maxInstant.isBefore(fileWatermark)) {
          maxInstant = fileWatermark;
        }
      }
      watermarkEstimator.setWatermark(maxInstant);
      return maxInstant.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE);
    }

    @GetInitialRestriction
    public DirectoryWatchRestriction initialRestriction() {
      return new DirectoryWatchRestriction(new HashSet<>(), false);
    }

    @GetRestrictionCoder
    public Coder<DirectoryWatchRestriction> getRestrictionCoder() {
      return new DirectoryWatchRestriction.Coder();
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState() {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    @GetWatermarkEstimatorStateCoder
    public Coder<Instant> getWatermarkEstimatorStateCoder() {
      return InstantCoder.of();
    }

    @NewWatermarkEstimator
    public WatermarkEstimators.Manual newWatermarkEstimator(
        @WatermarkEstimatorState Instant initialWatermark) {

      return new WatermarkEstimators.Manual(initialWatermark);
    }
  }

  @BoundedPerElement
  private static class FileReadFn extends DoFn<String, String> {

    @ProcessElement
    public void process(
        @Element String path,
        RestrictionTracker<OffsetRange, Long> tracker,
        OutputReceiver<String> outputReceiver)
        throws IOException {

      long position = tracker.currentRestriction().getFrom();
      try (RandomAccessFile file = new RandomAccessFile(path, "r")) {
        seekFileToStartLine(file, position);
        while (tracker.tryClaim(file.getFilePointer())) {
          outputReceiver.output(file.readLine());
        }
      }
    }

    @GetInitialRestriction
    public OffsetRange initialRestriction(@Element String path) throws IOException {
      return new OffsetRange(0, Files.size(Paths.get(path)));
    }

    @GetRestrictionCoder
    public Coder<OffsetRange> getRestrictionCoder() {
      return OffsetRange.Coder.of();
    }
  }

  private static List<KV<String, Instant>> getNewFilesIfAny(
      String path, RestrictionTracker<DirectoryWatchRestriction, String> tracker) {
    Preconditions.checkArgument(path != null);
    try (Stream<Path> stream = Files.list(Paths.get(path))) {
      return stream
          .filter(file -> !Files.isDirectory(file))
          .filter(f -> !tracker.currentRestriction().getAlreadyProcessed().contains(f.toString()))
          .map(
              f -> {
                try {
                  BasicFileAttributes attr = Files.readAttributes(f, BasicFileAttributes.class);
                  return KV.of(f.toString(), new Instant(attr.creationTime().toMillis()));
                } catch (IOException ex) {
                  throw new RuntimeException(ex);
                }
              })
          .collect(Collectors.toList());
    } catch (IOException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  private static void seekFileToStartLine(RandomAccessFile file, long position) throws IOException {
    final Byte precedingByte;
    if (position == 0) {
      precedingByte = null;
    } else {
      file.seek(position - 1);
      precedingByte = file.readByte();
    }
    if (precedingByte == null || '\n' == precedingByte) {
      return;
    }
    // discard the rest of current line, it will be processed in different offset range
    file.readLine();
  }

  private final String directoryPath;

  private StreamingFileRead(String directoryPath) {
    this.directoryPath = directoryPath;
  }

  @Override
  public PCollection<String> expand(PBegin input) {
    return input
        .apply(Impulse.create())
        .apply(MapElements.into(TypeDescriptors.strings()).via(e -> directoryPath))
        .apply(new DirectoryWatch())
        .apply(Reshuffle.viaRandomKey())
        .apply(new FileRead());
  }
}
