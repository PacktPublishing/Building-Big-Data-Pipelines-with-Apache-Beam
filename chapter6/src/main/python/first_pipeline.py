#!/usr/bin/env python3

import re
import sys
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def usage():
  sys.stderr.write("Usage: %s <input_file>\n" % (sys.argv[0], ))
  sys.exit(1)

if len(sys.argv) < 2:
  usage()

input_file = sys.argv[1]

with beam.Pipeline(options=PipelineOptions()) as p:
  (p | beam.io.ReadFromText(input_file)
    | "Tokenize" >> beam.FlatMap(lambda line: re.findall(r'[A-Za-z\']+', line))
    | "CountWords" >> beam.combiners.Count.PerElement()
    | "PrintOutput" >> beam.ParDo(lambda c: print(c)))
