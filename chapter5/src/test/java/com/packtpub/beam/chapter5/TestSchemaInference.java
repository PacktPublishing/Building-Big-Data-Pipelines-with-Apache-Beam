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
package com.packtpub.beam.chapter5;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.packtpub.beam.util.Position;
import lombok.Getter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Builder;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.beam.sdk.schemas.transforms.Cast;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.Filter;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.jupiter.api.Test;

public class TestSchemaInference {

  @DefaultSchema(JavaFieldSchema.class)
  public static class AutoSchemaPosition {

    @SchemaFieldName("latitude")
    @Getter
    final double _lat;

    @SchemaFieldName("longitude")
    @Getter
    final double _lon;

    @SchemaIgnore String ignore;

    @SchemaCreate
    public AutoSchemaPosition(double latitude, double longitude) {
      this._lat = latitude;
      this._lon = longitude;
    }
  }

  @Test
  public void testSchemaInference() {
    Pipeline p = Pipeline.create();
    PCollection<AutoSchemaPosition> positions =
        p.apply(Create.of(new AutoSchemaPosition(1.0, 2.0)));
    assertTrue(positions.hasSchema());
    PCollection<Row> rows =
        positions
            .apply(
                Filter.<AutoSchemaPosition>create()
                    .<Double>whereFieldName("latitude", f -> f > 0.0))
            .apply(Select.fieldNames("latitude", "longitude"));
    PCollection<Long> result =
        rows.apply(SqlTransform.query("SELECT COUNT(*) FROM PCOLLECTION"))
            .apply(Convert.to(Long.class));
    PAssert.that(result).containsInAnyOrder(1L);
    p.run();
  }

  @Test
  public void testManualSchema() {
    Pipeline p = Pipeline.create();
    Schema schema =
        new Builder()
            .addDoubleField("latitude")
            .addDoubleField("longitude")
            .addInt64Field("timestamp")
            .build();
    PCollection<Position> position =
        p.apply(
            Create.of(new Position(1.0, 2.0, System.currentTimeMillis()))
                .withSchema(
                    schema,
                    TypeDescriptor.of(Position.class),
                    pos ->
                        Row.withSchema(schema)
                            .withFieldValue("latitude", pos.getLatitude())
                            .withFieldValue("longitude", pos.getLongitude())
                            .withFieldValue("timestamp", pos.getTimestamp())
                            .build(),
                    r ->
                        new Position(
                            r.getDouble("latitude"),
                            r.getDouble("longitude"),
                            r.getInt64("timestamp"))));

    assertTrue(position.hasSchema());
    PCollection<Row> rows = position.apply(Cast.narrowing(position.getSchema()));
    PCollection<Long> result =
        rows.apply(SqlTransform.query("SELECT COUNT(*) c FROM PCOLLECTION"))
            .apply(MapElements.into(TypeDescriptors.longs()).via(r -> r.getInt64("c")));
    PAssert.that(result).containsInAnyOrder(1L);
    p.run();
  }
}
