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

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MapToLines<K, T>
    extends PTransform<PCollection<KafkaRecord<K, T>>, PCollection<String>> {

  public static <K, V> MapToLines<K, V> of() {
    return new MapToLines<>(true);
  }

  public static <K, V> MapToLines<K, V> ofValuesOnly() {
    return new MapToLines<>(false);
  }

  private final boolean includeKey;

  MapToLines(boolean includeKey) {
    this.includeKey = includeKey;
  }

  @Override
  public PCollection<String> expand(PCollection<KafkaRecord<K, T>> input) {
    return input.apply(
        MapElements.into(TypeDescriptors.strings())
            .via(
                r ->
                    includeKey
                        ? ifNotNull(r.getKV().getKey()) + " " + ifNotNull(r.getKV().getValue())
                        : ifNotNull(r.getKV().getValue())));
  }

  private static <T> String ifNotNull(T value) {
    return value != null ? value.toString() : "";
  }
}
