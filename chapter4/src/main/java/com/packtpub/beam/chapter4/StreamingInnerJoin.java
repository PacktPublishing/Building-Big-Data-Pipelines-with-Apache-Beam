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

import com.packtpub.beam.chapter4.JoinRow.JoinRowCoder;
import java.util.Arrays;
import java.util.Map.Entry;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;

public class StreamingInnerJoin<K, J, L, R>
    extends PTransform<PCollectionTuple, PCollection<JoinResult<K, J, L, R>>> {

  static class StreamInnerJoinDoFn<K, J, L, R>
      extends DoFn<KV<J, RawUnionValue>, JoinResult<K, J, L, R>> {

    private static final String LEFT_STATE = "left";
    private static final String RIGHT_STATE = "right";

    @FunctionalInterface
    private interface ProductCollector<K, J> {
      void collect(
          J joinKey,
          K primaryKey,
          Object primaryValue,
          K otherKey,
          Object otherValue,
          boolean retract);
    }

    @StateId(LEFT_STATE)
    final StateSpec<MapState<K, L>> leftStateSpec;

    @StateId(RIGHT_STATE)
    final StateSpec<MapState<K, R>> rightStateSpec;

    StreamInnerJoinDoFn(Coder<K> keyCoder, Coder<L> leftValueCoder, Coder<R> rightValueCoder) {
      leftStateSpec = StateSpecs.map(keyCoder, leftValueCoder);
      rightStateSpec = StateSpecs.map(keyCoder, rightValueCoder);
    }

    @RequiresTimeSortedInput
    @ProcessElement
    @SuppressWarnings("unchecked")
    public void processElement(
        @Element KV<J, RawUnionValue> element,
        @StateId(LEFT_STATE) MapState<K, L> left,
        @StateId(RIGHT_STATE) MapState<K, R> right,
        OutputReceiver<JoinResult<K, J, L, R>> output) {

      boolean isLeft = element.getValue().getUnionTag() == 0;
      JoinRow<K, J, Object> value = (JoinRow<K, J, Object>) element.getValue().getValue();
      K primaryKey = value.getKey();
      J joinKey = element.getKey();
      @Nullable Object inputValue = value.getValue();
      if (isLeft) {
        handleProduct(
            left,
            right,
            primaryKey,
            joinKey,
            (L) inputValue,
            (j, lk, lv, rk, rv, r) -> output.output(JoinResult.of(j, lk, (L) lv, rk, (R) rv, r)));
      } else {
        handleProduct(
            right,
            left,
            primaryKey,
            joinKey,
            (R) inputValue,
            (j, rk, rv, lk, lv, r) -> output.output(JoinResult.of(j, lk, (L) lv, rk, (R) rv, r)));
      }
    }

    private <P, O> void handleProduct(
        MapState<K, P> primary,
        MapState<K, O> other,
        K primaryKey,
        J joinKey,
        @Nullable P inputValue,
        ProductCollector<K, J> collector) {

      P oldValue = primary.get(primaryKey).read();
      Iterable<Entry<K, O>> otherValues = other.entries().read();
      if (oldValue != null) {
        otherValues.forEach(
            e -> collector.collect(joinKey, primaryKey, oldValue, e.getKey(), e.getValue(), true));
      }
      if (inputValue != null) {
        primary.put(primaryKey, (P) inputValue);
        otherValues.forEach(
            e ->
                collector.collect(
                    joinKey, primaryKey, inputValue, e.getKey(), e.getValue(), false));
      } else {
        primary.remove(primaryKey);
      }
    }
  }

  /**
   * Cache each row by primary key and compare join keys for changes. When join key changes a delete
   * is emitted to the previous join key to notify the downstream processor of the change.
   */
  private static class JoinKeyRetract<K, J, V>
      extends PTransform<PCollection<JoinRow<K, J, V>>, PCollection<JoinRow<K, J, V>>> {

    @Override
    public PCollection<JoinRow<K, J, V>> expand(PCollection<JoinRow<K, J, V>> input) {

      JoinRowCoder<K, J, V> coder = (JoinRowCoder<K, J, V>) input.getCoder();

      return input
          .apply(WithKeys.of(JoinRow::getKey))
          .setCoder(KvCoder.of(coder.getKeyCoder(), coder))
          .apply("cachePrimaryKey", ParDo.of(new CacheAndUpdateJoinKeyFn<>(coder)));
    }
  }

  /** The actual caching. */
  private static class CacheAndUpdateJoinKeyFn<K, J, V>
      extends DoFn<KV<K, JoinRow<K, J, V>>, JoinRow<K, J, V>> {

    private static final String STATE_ID = "cache";

    private final Coder<JoinRow<K, J, V>> rowCoder;

    @StateId(STATE_ID)
    private final StateSpec<ValueState<JoinRow<K, J, V>>> cacheSpec;

    CacheAndUpdateJoinKeyFn(Coder<JoinRow<K, J, V>> rowCoder) {
      this.rowCoder = rowCoder;
      cacheSpec = StateSpecs.value(this.rowCoder);
    }

    @RequiresTimeSortedInput
    @ProcessElement
    public void processElement(
        @Element KV<K, JoinRow<K, J, V>> element,
        @StateId(STATE_ID) ValueState<JoinRow<K, J, V>> cache,
        OutputReceiver<JoinRow<K, J, V>> output) {

      JoinRow<K, J, V> cached = cache.read();
      if (cached != null
          && cached.getValue() != null
          && !cached.getJoinKey().equals(element.getValue().getJoinKey())) {
        // join key was changed, we need to delete the value in the original
        // downstream processor
        output.output(cached.toDelete());
      }
      cache.write(element.getValue());
      output.output(element.getValue());
    }
  }

  private final TupleTag<L> leftHahdTag;
  private final TupleTag<R> rightHandTag;
  private final SerializableFunction<L, K> leftKeyExtractor;
  private final SerializableFunction<R, K> rightKeyExtractor;
  private final SerializableFunction<L, J> leftJoinKeyExtractor;
  private final SerializableFunction<R, J> rightJoinKeyExtractor;
  private final Coder<K> keyCoder;
  private final Coder<J> joinKeyCoder;

  StreamingInnerJoin(
      TupleTag<L> leftHandTag,
      TupleTag<R> rightHandTag,
      SerializableFunction<L, K> leftKeyExtractor,
      SerializableFunction<R, K> rightKeyExtractor,
      SerializableFunction<L, J> leftJoinKeyExtractor,
      SerializableFunction<R, J> rightJoinKeyExtractor,
      Coder<K> keyCoder,
      Coder<J> joinKeyCoder) {

    this.leftHahdTag = leftHandTag;
    this.rightHandTag = rightHandTag;
    this.leftKeyExtractor = leftKeyExtractor;
    this.rightKeyExtractor = rightKeyExtractor;
    this.leftJoinKeyExtractor = leftJoinKeyExtractor;
    this.rightJoinKeyExtractor = rightJoinKeyExtractor;
    this.keyCoder = keyCoder;
    this.joinKeyCoder = joinKeyCoder;
  }

  @SuppressWarnings("unchecked")
  @Override
  public PCollection<JoinResult<K, J, L, R>> expand(PCollectionTuple input) {
    PCollection<L> leftInput = input.get(leftHahdTag);
    PCollection<R> rightInput = input.get(rightHandTag);

    PCollection<JoinRow<K, J, L>> leftRow =
        leftInput
            .apply(mapToJoinRow(leftKeyExtractor, leftJoinKeyExtractor))
            .setCoder(JoinRow.coder(keyCoder, joinKeyCoder, leftInput.getCoder()))
            .apply("retractLeftUpdates", new JoinKeyRetract<>())
            .setCoder(JoinRow.coder(keyCoder, joinKeyCoder, leftInput.getCoder()));

    PCollection<JoinRow<K, J, R>> rightRow =
        rightInput
            .apply(mapToJoinRow(rightKeyExtractor, rightJoinKeyExtractor))
            .setCoder(JoinRow.coder(keyCoder, joinKeyCoder, rightInput.getCoder()))
            .apply("retractRightUpdates", new JoinKeyRetract<>())
            .setCoder(JoinRow.coder(keyCoder, joinKeyCoder, rightInput.getCoder()));

    UnionCoder unionCoder = UnionCoder.of(Arrays.asList(leftRow.getCoder(), rightRow.getCoder()));
    PCollection<RawUnionValue> leftRawUnion =
        leftRow
            .apply(
                MapElements.into(TypeDescriptor.of(RawUnionValue.class))
                    .via(e -> new RawUnionValue(0, e)))
            .setCoder(unionCoder);

    PCollection<RawUnionValue> rightRawUnion =
        rightRow
            .apply(
                MapElements.into(TypeDescriptor.of(RawUnionValue.class))
                    .via(e -> new RawUnionValue(1, e)))
            .setCoder(unionCoder);

    return PCollectionList.of(leftRawUnion)
        .and(rightRawUnion)
        .apply(Flatten.pCollections())
        .apply(WithKeys.of(val -> ((JoinRow<K, J, ?>) val.getValue()).getJoinKey()))
        .setCoder(KvCoder.of(joinKeyCoder, unionCoder))
        .apply(
            "innerJoin",
            ParDo.of(
                new StreamInnerJoinDoFn<>(keyCoder, leftInput.getCoder(), rightInput.getCoder())))
        .setCoder(
            JoinResult.coder(keyCoder, joinKeyCoder, leftInput.getCoder(), rightInput.getCoder()));
  }

  @SuppressWarnings("unchecked")
  private <T> PTransform<PCollection<T>, PCollection<JoinRow<K, J, T>>> mapToJoinRow(
      SerializableFunction<T, K> keyExtractor, SerializableFunction<T, J> joinKeyExtractor) {

    return (PTransform)
        MapElements.into(new TypeDescriptor<JoinRow<K, J, T>>(JoinRow.class) {})
            .<T>via(e -> new JoinRow<>(keyExtractor.apply(e), joinKeyExtractor.apply(e), e));
  }
}
