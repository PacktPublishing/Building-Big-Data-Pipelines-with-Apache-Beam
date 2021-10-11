#!/usr/bin/env python3

import sys

import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.kafka import WriteToKafka
from apache_beam import window
from apache_beam import trigger
from beam_utils import get_expansion_service

def usage():
  sys.stderr.write("Usage: %s <bootstrap_server> <input_topic> <output_topic>\n" % (sys.argv[0], ))
  sys.exit(1)

@beam.ptransform_fn
def ComputeLongestWord(input):
  return (input
      | "Tokenize" >> beam.FlatMap(lambda line: re.findall(r'[A-Za-z\']+', line))
      | beam.WindowInto(
          window.GlobalWindows(),
          trigger=trigger.AfterWatermark(early=trigger.AfterCount(1)),
          accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
          allowed_lateness=window.Duration.of(0))
      | "MaxLength" >> beam.combiners.Top.Of(1, key=len).without_defaults()
      | "Flatten" >> beam.FlatMap(lambda x: x))

def toKv(s: str) -> beam.typehints.KV[bytes, bytes]:
  return ("".encode("utf-8"), s.encode("utf-8"))

if __name__ == "__main__":
  if len(sys.argv) < 4:
    usage()

  bootstrapServer, inputTopic, outputTopic = sys.argv[1:4]

  with beam.Pipeline(options=PipelineOptions(["--streaming"] + sys.argv[4:])) as p:
    (p | ReadFromKafka(
        consumer_config={'bootstrap.servers': bootstrapServer},
        topics=[inputTopic],
        expansion_service=get_expansion_service())
     | "ToLines" >> beam.Map(lambda x: "%s %s" % (x[0].decode("utf-8"), x[1].decode("utf-8")))
     | "ComputeLongestWord" >> ComputeLongestWord()
     | beam.Map(toKv)
     | "StoreOutput" >> WriteToKafka(
         producer_config={'bootstrap.servers': bootstrapServer},
         topic=outputTopic,
         expansion_service=get_expansion_service()))

