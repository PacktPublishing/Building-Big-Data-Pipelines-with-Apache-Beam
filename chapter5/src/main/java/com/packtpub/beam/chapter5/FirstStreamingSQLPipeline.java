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

import com.packtpub.beam.util.PrintElements;
import com.packtpub.beam.util.Utils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

public class FirstStreamingSQLPipeline {

  public static void main(String[] args) throws IOException {

    // read some input from text file
    ClassLoader loader = FirstSQLPipeline.class.getClassLoader();
    String file = loader.getResource("lorem.txt").getFile();
    List<String> lines = Files.readAllLines(Paths.get(file), StandardCharsets.UTF_8);

    Pipeline pipeline = Pipeline.create();
    Schema lineSchema =
        Schema.of(Field.of("word", FieldType.STRING), Field.of("ts", FieldType.DATETIME));

    TestStream.Builder<Row> streamBuilder = TestStream.create(lineSchema);
    Instant now = Instant.now();
    List<Row> inputRows =
        IntStream.range(0, lines.size())
            .mapToObj(i -> i)
            .flatMap(
                i ->
                    Utils.toWords(lines.get(i))
                        .stream()
                        .map(
                            w ->
                                Row.withSchema(lineSchema)
                                    .withFieldValue("word", w)
                                    .withFieldValue("ts", now.plus(i))
                                    .build()))
            .collect(Collectors.toList());
    for (Row row : inputRows) {
      streamBuilder = streamBuilder.addElements(row);
    }

    // create the unbounded PCollection from TestStream
    PCollection<Row> input = pipeline.apply(streamBuilder.advanceWatermarkToInfinity());

    PCollection<KV<String, Long>> result =
        input
            .apply(
                SqlTransform.query(
                    "SELECT word, COUNT(*) as c FROM PCOLLECTION GROUP BY word, TUMBLE(ts, INTERVAL '1' DAY)"))
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                    .via(r -> KV.of(r.getString("word"), r.getInt64("c"))));

    // print the contents of PCollection 'result' to standard out
    result.apply(PrintElements.of());

    // run the Pipeline
    pipeline.run().waitUntilFinish();
  }
}
