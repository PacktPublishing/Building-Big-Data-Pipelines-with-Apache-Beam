#!/usr/bin/env python3

import sys

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.kafka import WriteToKafka
from apache_beam import window
from apache_beam import trigger
from beam_utils import get_expansion_service

def usage():
  sys.stderr.write(
      "Usage: %s <bootstrapServer> <inputTopic> <outputTopic>\n"
      % (sys.argv[0], ))
  sys.exit(1)

@beam.ptransform_fn
def SportTrackerCalc(input):

  import math

  EARTH_DIAMETER = 6_371_000 # meters

  def calculateDelta(deltaLatLon):
    return math.sqrt(2 * (1 - math.cos(deltaLatLon)))

  def distance(p1, p2):
    deltaLatitude = (p1[0] - p2[0]) * math.pi / 180
    deltaLongitude = (p1[1] - p2[1]) * math.pi / 180
    latitudeIncInMeters = calculateDelta(deltaLatitude)
    longitudeIncInMeters = calculateDelta(deltaLongitude)
    return EARTH_DIAMETER * math.sqrt(
      latitudeIncInMeters * latitudeIncInMeters + longitudeIncInMeters * longitudeIncInMeters)

  def computeMetrics(key, trackPositions):
    last = None
    totalTime = 0
    totalDistance = 0
    for p in sorted(trackPositions, key=lambda x: x[2]):
      if last != None:
        totalDistance += distance(last, p)
        totalTime += p[2] - last[2]
      last = p
    return (key, totalTime, totalDistance)


  return (input
      | beam.WindowInto(
          window.GlobalWindows(),
          trigger=trigger.AfterWatermark(
            early=trigger.AfterProcessingTime(10)),
          timestamp_combiner=window.TimestampCombiner.OUTPUT_AT_LATEST,
          accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
          allowed_lateness=window.Duration.of(0))
      | "GroupByWorkout" >> beam.GroupByKey()
      | "ComputeMetrics" >> beam.Map(lambda x: computeMetrics(*x)))

def toKv(t: beam.typehints.Tuple[str, float, float]) -> beam.typehints.KV[str, str]:
  return ("", "%s\t%f\t%f" % t)

def toPositions(x, stamp=beam.DoFn.TimestampParam):
  return (
    x[0].decode("utf-8"),
    tuple(map(float, x[1].decode("utf-8").split(" "))) + (stamp.micros / 1000., ))

if __name__ == "__main__":
  if len(sys.argv) < 4:
    usage()

  bootstrapServer, inputTopic, outputTopic = sys.argv[1:4]

  with beam.Pipeline(options=PipelineOptions(["--streaming"] + sys.argv[4:])) as p:
    (p | ReadFromKafka(
        consumer_config={'bootstrap.servers': bootstrapServer},
        topics=[inputTopic],
        timestamp_policy=ReadFromKafka.create_time_policy,
        expansion_service=get_expansion_service())
     | "ToPositions" >> beam.Map(toPositions)
     | "SportTracker" >> SportTrackerCalc()
     | "ToKv" >> beam.Map(toKv)
     | "StoreOutput" >> WriteToKafka(
         producer_config={'bootstrap.servers': bootstrapServer},
         topic=outputTopic,
         key_serializer="org.apache.kafka.common.serialization.StringSerializer",
         value_serializer="org.apache.kafka.common.serialization.StringSerializer",
         expansion_service=get_expansion_service()))
