#!/usr/bin/env python3

import sys

import apache_beam
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.kafka import WriteToKafka
from apache_beam import window
from apache_beam.transforms.userstate import *
from apache_beam.transforms import DoFn

def usage():
  sys.stderr.write(
    "Usage: %s <bootstrapServer> <inputTopic> <outputTopic>"
    % (sys.argv[0], ))
  sys.exit(1)

class MeanPaceCombineFn(beam.core.CombineFn):

  def create_accumulator(self):
    return (0, 0)

  def add_input(self, acc, element):
    return tuple(map(sum, zip(acc, element)))

  def merge_accumulators(self, accumulators):
    return tuple(map(sum, zip(*accumulators)))

  def extract_output(self, acc):
    (distance, time) = acc
    if time == 0:
      return float('NaN')
    return distance / float(time)

@beam.ptransform_fn
def CalculateAveragePace(input):
  return input | beam.core.CombinePerKey(MeanPaceCombineFn())

@beam.typehints.with_input_types(
    beam.typehints.Tuple[str, beam.typehints.Tuple[float, float, float]])
class ToMetricFn(DoFn):

  BUFFER = BagStateSpec(
      "buffer",
      beam.coders.TupleCoder([
          beam.coders.FloatCoder(),
          beam.coders.FloatCoder(),
          beam.coders.FloatCoder()]))
  MIN_STAMP = ReadModifyWriteStateSpec("minStamp", beam.coders.FloatCoder())
  FLUSH_TIMER = TimerSpec("flush", TimeDomain.WATERMARK)

  def __init__(self, duration):
    self.duration = duration

  def process(
      self,
      element,
      stamp=DoFn.TimestampParam,
      buffer=DoFn.StateParam(BUFFER),
      minStamp=DoFn.StateParam(MIN_STAMP),
      flushTimer=DoFn.TimerParam(FLUSH_TIMER)):

    currentMinStamp = minStamp.read() or stamp
    if currentMinStamp == stamp:
      minStamp.write(stamp)
      flushTimer.set(stamp)
    buffer.add(element[1])

  @on_timer(FLUSH_TIMER)
  def flush(
      self,
      stamp=DoFn.TimestampParam,
      key=DoFn.KeyParam,
      buffer=DoFn.StateParam(BUFFER),
      minStamp=DoFn.StateParam(MIN_STAMP),
      flushTimer=DoFn.TimerParam(FLUSH_TIMER)):

    keep, flush = [], []
    minKeepStamp = None
    for item in buffer.read():
      if item[2] <= stamp:
        flush.append(item)
      else:
        keep.append(item)
        if not minKeepStamp or minKeepStamp > item[2]:
          minKeepStamp = item[2]

    outputs = []
    if flush:
      flush = list(sorted(flush, key=lambda x: x[2]))
      outputs = list(self.flushMetrics(flush, key))
      keep.append(flush[-1])

    buffer.clear()
    for item in keep:
      buffer.add(item)
    if minKeepStamp:
      flushTimer.set(minKeepStamp)
      minStamp.write(minKeepStamp)
    else:
      minStamp.clear()

    return outputs

  def flushMetrics(self, flush, key):

    from utils import distance, move
    import apache_beam as beam

    i = 1
    while i < len(flush):
      last = flush[i - 1]
      next = flush[i]
      direction = (next[0] - last[0], next[1] - last[1])
      timeDiff = next[2] - last[2]
      if timeDiff > 0:
        avgSpeed = distance(next, last) / timeDiff
        while next[2] > last[2]:
          delta = min(next[2] - last[2], 60 - last[2] % 60)
          n = move(last, direction, avgSpeed, delta)
          d = distance(n, last)
          yield beam.window.TimestampedValue((key, (d, delta)), last[2])
          last = n
      i += 1

@apache_beam.ptransform_fn
def ComputeBoxedMetrics(input, duration):
  return (input
      | beam.WindowInto(window.GlobalWindows())
      | beam.ParDo(ToMetricFn(duration)))

def asMotivation(x):
  if not x[1][0] or not x[1][1] or 0.9 < x[1][0][0] / x[1][1][0] < 1.1:
    return []
  return [(x[0], x[1][0][0] > x[1][1][0])]

@beam.ptransform_fn
@beam.typehints.with_input_types(
  beam.typehints.Tuple[str, beam.typehints.Tuple[float, float, float]])
@beam.typehints.with_output_types(beam.typehints.Tuple[str, bool])
def SportTrackerMotivation(input, shortDuration, longDuration):

  boxed = input | "ComputeMetrics" >> ComputeBoxedMetrics(shortDuration)
  shortAverage = (boxed
      | "shortWindow" >> beam.WindowInto(window.FixedWindows(shortDuration))
      | "shortAverage" >> CalculateAveragePace())
  longAverage = (boxed
      | "longWindow" >> beam.WindowInto(window.SlidingWindows(longDuration, shortDuration))
      | "longAverage" >> CalculateAveragePace()
      | "longIntoFixed" >> beam.WindowInto(window.FixedWindows(shortDuration)))
  return ((shortAverage, longAverage)
          | beam.CoGroupByKey()
          | beam.FlatMap(asMotivation))

def toPositions(
    x: beam.typehints.Tuple[bytes, bytes], stamp=beam.DoFn.TimestampParam) -> \
        beam.typehints.Tuple[str, beam.typehints.Tuple[float, float, float]]:
  return (
    x[0].decode("utf-8"),
    tuple(map(float, x[1].decode("utf-8").split(" "))) + (stamp.micros / 1000000., ))

def prepareToKafka(t: beam.typehints.Tuple[str, bool]) -> beam.typehints.KV[str, str]:
  return (
    "",
    ("Good job %s, you are gaining speed!" % (t[0], ) if t[1]
        else "Looks like you are slowing down user %s. Go, go, go!" % (t[0], )))

if __name__ == "__main__":

  from beam_utils import get_expansion_service

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
     | "SportTrackerMotivation" >> SportTrackerMotivation(60, 300)
     | "MapToKafka" >> beam.Map(prepareToKafka)
     | "StoreOutput" >> WriteToKafka(
        producer_config={'bootstrap.servers': bootstrapServer},
        topic=outputTopic,
        key_serializer="org.apache.kafka.common.serialization.StringSerializer",
        value_serializer="org.apache.kafka.common.serialization.StringSerializer",
        expansion_service=get_expansion_service()))

