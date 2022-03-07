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
import lombok.Getter;
import lombok.Value;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.NullableCoder;

@Value
public class JoinRow<K, J, V> {

  public static <K, J, V> JoinRowCoder<K, J, V> coder(
      Coder<K> keyCoder, Coder<J> joinKeyCoder, Coder<V> valueCoder) {

    return new JoinRowCoder<>(keyCoder, joinKeyCoder, valueCoder);
  }

  public static class JoinRowCoder<K, J, V> extends CustomCoder<JoinRow<K, J, V>> {

    @Getter private final Coder<K> keyCoder;
    @Getter private final Coder<J> joinKeyCoder;
    @Getter private final Coder<V> valueCoder;

    private JoinRowCoder(Coder<K> keyCoder, Coder<J> joinKeyCoder, Coder<V> valueCoder) {
      this.keyCoder = keyCoder;
      this.joinKeyCoder = joinKeyCoder;
      this.valueCoder = NullableCoder.of(valueCoder);
    }

    @Override
    public void encode(JoinRow<K, J, V> value, OutputStream outStream)
        throws CoderException, IOException {

      keyCoder.encode(value.getKey(), outStream);
      joinKeyCoder.encode(value.getJoinKey(), outStream);
      valueCoder.encode(value.getValue(), outStream);
    }

    @Override
    public JoinRow<K, J, V> decode(InputStream inStream) throws CoderException, IOException {
      return new JoinRow<>(
          keyCoder.decode(inStream), joinKeyCoder.decode(inStream), valueCoder.decode(inStream));
    }
  }

  K key;
  J joinKey;
  @Nullable V value;

  public JoinRow<K, J, V> toDelete() {
    return new JoinRow<>(key, joinKey, null);
  }
}
