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
package com.packtpub.beam.chapter1;

import com.packtpub.beam.util.PrintElements;
import com.packtpub.beam.util.Tokenize;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class FirstPipeline {

  public static void main(String[] args) throws IOException {

    // read some input from text file
    ClassLoader loader = Chapter1Demo.class.getClassLoader();
    String file = loader.getResource("lorem.txt").getFile();
    List<String> lines = Files.readAllLines(Paths.get(file), StandardCharsets.UTF_8);

    // create empty Pipeline
    // A Pipeline is a container for both data (PCollection)
    // and operations (PTransforms)
    Pipeline pipeline = Pipeline.create();

    // transform 'lines' into PCollection
    PCollection<String> input = pipeline.apply(Create.of(lines));

    // tokenize the input into words
    PCollection<String> words = input.apply(Tokenize.of());

    // count each occurrence of a word
    PCollection<KV<String, Long>> result = words.apply(Count.perElement());

    // print the contents of PCollection 'result' to standard out
    result.apply(PrintElements.of());

    // run the Pipeline
    pipeline.run().waitUntilFinish();
  }
}
