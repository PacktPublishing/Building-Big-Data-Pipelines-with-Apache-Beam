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
package com.packtpub.beam.chapter2;

import static com.packtpub.beam.chapter2.AverageWordLength.calculateAverageWordLength;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;

public class AverageWordLengthTest {

  @Test
  public void testAverageWordCalculation() {
    Pipeline pipeline = Pipeline.create();
    PCollection<String> input =
        pipeline.apply(
            TestStream.create(StringUtf8Coder.of())
                .addElements("a")
                .addElements("bb")
                .addElements("ccc")
                .advanceWatermarkToInfinity());
    PCollection<Double> averages = calculateAverageWordLength(input);
    PAssert.that(averages).containsInAnyOrder(1.0, 1.5, 2.0);
    pipeline.run();
  }
}
