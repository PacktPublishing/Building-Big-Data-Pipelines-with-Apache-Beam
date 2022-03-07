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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.VarLongCoder;

public class PositionCoder extends CustomCoder<Position> {

  private static final DoubleCoder DOUBLE_CODER = DoubleCoder.of();
  private static final VarLongCoder LONG_CODER = VarLongCoder.of();

  @Override
  public void encode(Position value, OutputStream outStream) throws CoderException, IOException {
    DOUBLE_CODER.encode(value.getLatitude(), outStream);
    DOUBLE_CODER.encode(value.getLongitude(), outStream);
    LONG_CODER.encode(value.getTimestamp(), outStream);
  }

  @Override
  public Position decode(InputStream inStream) throws CoderException, IOException {
    return new Position(
        DOUBLE_CODER.decode(inStream), DOUBLE_CODER.decode(inStream), LONG_CODER.decode(inStream));
  }
}
