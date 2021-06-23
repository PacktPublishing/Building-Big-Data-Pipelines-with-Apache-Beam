import unittest

from max_word_length import ComputeLongestWord
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

class TestMaxWordLength(unittest.TestCase):

  def test_pipeline(self):
    input = ["a bb ccc", "longer", "longest short"]
    with TestPipeline() as p:
        res = (p
               | beam.Create(input)
               | ComputeLongestWord())
        assert_that(res, equal_to(["longest", "longest"]))

if __name__ == '__main__':
    unittest.main()
