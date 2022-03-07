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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;

@Value
public class JoinResult<K, J, L, R> {

  public static <K, J, L, R> JoinResult<K, J, L, R> of(
      J joinKey,
      @Nullable K leftKey,
      @Nullable L leftValue,
      @Nullable K rightKey,
      @Nullable R rightValue,
      boolean isRetract) {

    return new JoinResult<>(joinKey, isRetract, leftKey, rightKey, leftValue, rightValue);
  }

  public static class JoinResultCoder<K, J, L, R> extends CustomCoder<JoinResult<K, J, L, R>> {

    private static final BooleanCoder BOOLEAN_CODER = BooleanCoder.of();
    private final Coder<K> keyCoder;
    private final Coder<J> joinKeyCoder;
    private final Coder<L> leftValueCoder;
    private final Coder<R> rightValueCoder;

    public JoinResultCoder(
        Coder<K> keyCoder,
        Coder<J> joinKeyCoder,
        Coder<L> leftValueCoder,
        Coder<R> rightValueCoder) {
      this.keyCoder = keyCoder;
      this.joinKeyCoder = joinKeyCoder;
      this.leftValueCoder = leftValueCoder;
      this.rightValueCoder = rightValueCoder;
    }

    @Override
    public void encode(JoinResult<K, J, L, R> value, OutputStream outStream)
        throws CoderException, IOException {
      joinKeyCoder.encode(value.getJoinKey(), outStream);
      BOOLEAN_CODER.encode(value.isRetract(), outStream);
      if (value.getLeftValue() != null) {
        BOOLEAN_CODER.encode(true, outStream);
        keyCoder.encode(value.getLeftPrimaryKey(), outStream);
        leftValueCoder.encode(value.getLeftValue(), outStream);
      } else {
        BOOLEAN_CODER.encode(false, outStream);
      }
      if (value.getRightValue() != null) {
        BOOLEAN_CODER.encode(true, outStream);
        keyCoder.encode(value.getRightPrimaryKey(), outStream);
        rightValueCoder.encode(value.getRightValue(), outStream);
      } else {
        BOOLEAN_CODER.encode(false, outStream);
      }
    }

    @Override
    public JoinResult<K, J, L, R> decode(InputStream inStream) throws CoderException, IOException {
      J joinKey = joinKeyCoder.decode(inStream);
      boolean isRetract = BOOLEAN_CODER.decode(inStream);
      K leftKey = null;
      L leftValue = null;
      K rightKey = null;
      R rightValue = null;
      if (BOOLEAN_CODER.decode(inStream)) {
        leftKey = keyCoder.decode(inStream);
        leftValue = leftValueCoder.decode(inStream);
      }
      if (BOOLEAN_CODER.decode(inStream)) {
        rightKey = keyCoder.decode(inStream);
        rightValue = rightValueCoder.decode(inStream);
      }
      return new JoinResult<>(joinKey, isRetract, leftKey, rightKey, leftValue, rightValue);
    }
  }

  public static <K, J, L, R> JoinResultCoder<K, J, L, R> coder(
      Coder<K> keyCoder, Coder<J> joinKeyCoder, Coder<L> leftValueCoder, Coder<R> rightValueCoder) {

    return new JoinResultCoder<>(keyCoder, joinKeyCoder, leftValueCoder, rightValueCoder);
  }

  J joinKey;
  boolean retract;
  @Nullable K leftPrimaryKey;
  @Nullable K rightPrimaryKey;
  @Nullable L leftValue;
  @Nullable R rightValue;
}
