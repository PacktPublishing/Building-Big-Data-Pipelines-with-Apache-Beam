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
import com.packtpub.beam.util.Tokenize;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

public class FirstSQLPipeline {

  public static void main(String[] args) throws IOException {

    // read some input from text file
    ClassLoader loader = FirstSQLPipeline.class.getClassLoader();
    String file = loader.getResource("lorem.txt").getFile();
    List<String> lines = Files.readAllLines(Paths.get(file), StandardCharsets.UTF_8);

    Pipeline pipeline = Pipeline.create();
    Schema lineSchema = Schema.of(Field.of("s", FieldType.STRING));

    PCollection<String> input =
        pipeline
            .apply(
                Create.of(lines)
                    .withSchema(
                        lineSchema,
                        TypeDescriptors.strings(),
                        s -> Row.withSchema(lineSchema).attachValues(s),
                        r -> r.getString("s")))
            .apply(Tokenize.of());

    PCollection<KV<String, Long>> result =
        input
            .apply(Convert.toRows())
            .apply(
                SqlTransform.query("SELECT s as word, COUNT(*) as c FROM PCOLLECTION GROUP BY s"))
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
