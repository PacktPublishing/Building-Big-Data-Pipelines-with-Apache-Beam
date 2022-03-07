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

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;

public class WithStringSchemaTest {

  @Test
  public void testAddSchema() {
    Pipeline p = Pipeline.create();
    PCollection<String> input = p.apply(Create.of("foo", "bar"));
    PCollection<String> result = input.apply(WithStringSchema.of("value"));
    assertTrue(result.hasSchema());
  }
}
