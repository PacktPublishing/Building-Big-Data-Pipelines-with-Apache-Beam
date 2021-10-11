#!/usr/bin/env python3

import re
import sys
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam import window
from apache_beam import trigger

def usage():
  sys.stderr.write("Usage: %s <input_file>\n" % (sys.argv[0], ))
  sys.exit(1)

def readToStream(filename):
  lines = map(lambda line: line.strip(), open(filename, "r").read().split("\n"))
  return TestStream() \
      .add_elements(lines) \
      .advance_watermark_to_infinity()

if len(sys.argv) < 2:
  usage()

input_file = sys.argv[1]

with beam.Pipeline(options=PipelineOptions(["--streaming"])) as p:
  (p | readToStream(input_file)
    | beam.WindowInto(
          window.GlobalWindows(),
          trigger=trigger.AfterWatermark(),
          accumulation_mode=trigger.AccumulationMode.DISCARDING,
          allowed_lateness=window.Duration.of(0))
    | "Tokenize" >> beam.FlatMap(lambda line: re.findall(r'[A-Za-z\']+', line))
    | "CountWords" >> beam.combiners.Count.PerElement()
    | "PrintOutput" >> beam.ParDo(lambda c: print(c)))
