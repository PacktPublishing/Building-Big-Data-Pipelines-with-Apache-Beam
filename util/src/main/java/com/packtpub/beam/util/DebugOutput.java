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

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

public class DebugOutput<T> extends PTransform<PCollection<T>, PCollection<T>> {

  public static <T> DebugOutput<T> create(String name) {
    return new DebugOutput<>(name);
  }

  private final String name;

  private DebugOutput(String name) {
    this.name = name;
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    return input.apply(ParDo.of(new DebugOutputFn()));
  }

  private class DebugOutputFn extends DoFn<T, T> {
    @ProcessElement
    public void process(
        @Element T elem, @Timestamp Instant ts, BoundedWindow window, OutputReceiver<T> output) {

      System.out.println("DEBUG: " + name + ": " + elem + ", ts: " + ts + " window: " + window);
      output.output(elem);
    }
  }
}
