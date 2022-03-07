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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Iterables;
import java.util.Random;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;

public class PiSamplerTest {

  @Test
  public void testSampler() {
    Pipeline p = Pipeline.create();
    PCollection<Double> result =
        p.apply(PiSampler.of(10000, 10).withRandomFactory(() -> new Random(0)));
    PAssert.that(result)
        .satisfies(
            values -> {
              Double res = Iterables.getOnlyElement(values);
              assertEquals(res, Math.PI, 0.01);
              return null;
            });
    p.run();
  }
}
