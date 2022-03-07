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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.junit.jupiter.api.Test;

public class MyStringCoderTest {

  @Test
  public void testCoder() throws IOException {
    Coder<String> coder = new MyStringCoder();
    byte[] serialized;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      coder.encode("This is string. \uD83D\uDE0A", baos);
      baos.flush();
      serialized = baos.toByteArray();
    }
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized)) {
      assertEquals("This is string. \uD83D\uDE0A", coder.decode(bais));
    }
  }
}
