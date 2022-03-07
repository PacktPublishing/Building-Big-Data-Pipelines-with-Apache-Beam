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
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class ProcessingTimeWindow {

  public static void main(String[] args) throws IOException {
    // read some input from text file
    ClassLoader loader = Chapter1Demo.class.getClassLoader();
    String file = loader.getResource("lorem.txt").getFile();
    List<String> lines = Files.readAllLines(Paths.get(file), StandardCharsets.UTF_8);

    // create empty Pipeline
    Pipeline pipeline = Pipeline.create();

    // transform 'lines' into PCollection
    PCollection<String> input = pipeline.apply(Create.of(lines));

    // tokenize the input into words
    PCollection<String> words =
        input.apply(Tokenize.of()).apply(WithReadDelay.ofProcessingTime(Duration.millis(20)));

    // Window into single window and specify trigger
    PCollection<String> windowed =
        words.apply(
            Window.<String>into(new GlobalWindows())
                .triggering(
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.standardSeconds(1))))
                .discardingFiredPanes());

    // count each occurrence of every single element
    PCollection<Long> result = windowed.apply(Count.globally());

    // print the contents of PCollection 'result' to standard out
    result.apply(PrintElements.of());

    // run the Pipeline
    pipeline.run().waitUntilFinish();
  }
}
