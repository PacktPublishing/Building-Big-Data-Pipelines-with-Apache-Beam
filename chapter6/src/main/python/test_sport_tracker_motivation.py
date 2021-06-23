import typing
import unittest

import time
import random
from sport_tracker_motivation import SportTrackerMotivation
import apache_beam as beam
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils.timestamp import Timestamp
from utils import move

def randomPosition(stamp):
   return ((random.random() - 0.5) * 180.0, (random.random() - 0.5) * 180.0, stamp)

class TestSportTrackerMotivation(unittest.TestCase):

  def test_pipeline_bounded(self):
    now = time.time()
    now = now - now % 60
    inputs = [("foo", randomPosition(now)), ("bar", randomPosition(now))]
    inputs.append(("foo", move(inputs[0][1], (1.0, 1.0), 3, 180)))
    inputs.append(("bar", move(inputs[1][1], (1.0, 1.0), 2, 240)))
    inputs.append(("foo", move(inputs[2][1], (1.0, 1.0), 2.5, 180)))
    inputs.append(("bar", move(inputs[3][1], (1.0, 1.0), 2.5, 120)))

    stream = TestStream()
    for item in inputs:
      stream = stream.add_elements([item], event_timestamp=item[1][2])
    with beam.Pipeline() as p:
        res = (p
            | stream.advance_watermark_to_infinity()
               .with_output_types(typing.Tuple[str, typing.Tuple[float, float, float]])
            | SportTrackerMotivation(60, 300))
        assert_that(res, equal_to([("foo", False), ("bar", True), ("foo", False), ("bar", True)]))

  def test_pipeline_unbounded(self):
    now = time.time()
    now = now - now % 60
    inputs = [("foo", randomPosition(now)), ("bar", randomPosition(now))]
    inputs.append(("foo", move(inputs[0][1], (1.0, 1.0), 3, 180)))
    inputs.append(("bar", move(inputs[1][1], (1.0, 1.0), 2, 240)))
    inputs.append(("foo", move(inputs[2][1], (1.0, 1.0), 2.5, 180)))
    inputs.append(("bar", move(inputs[3][1], (1.0, 1.0), 2.5, 120)))

    stream = TestStream().advance_watermark_to(Timestamp.of(now))
    watermarks = [now + 100, now + 110, now + 119, now + 120]
    for item in inputs:
      stream = stream.add_elements([item], event_timestamp=item[1][2])
      if watermarks:
        stream.advance_watermark_to(Timestamp.of(watermarks.pop(0)))
    with beam.Pipeline() as p:
      res = (p
             | stream.advance_watermark_to_infinity()
             .with_output_types(typing.Tuple[str, typing.Tuple[float, float, float]])
             | SportTrackerMotivation(60, 300))
      assert_that(res, equal_to([("foo", False), ("bar", True), ("foo", False), ("bar", True)]))

if __name__ == '__main__':
    unittest.main()
