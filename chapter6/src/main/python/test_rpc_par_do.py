
import unittest

import grpc

import service_pb2
import service_pb2_grpc
from rpc_par_do import RPCServer

from rpc_par_do import RPCParDoStateful
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

class TestRPCParDo(unittest.TestCase):

  def test_server(self):
    with RPCServer(1234):
      with grpc.insecure_channel("localhost:1234") as channel:
        stub = service_pb2_grpc.RpcServiceStub(channel)
        req = service_pb2.RequestList()
        req.request.extend([service_pb2.Request(input="ccc")])
        res = stub.resolveBatch(req)
        self.assertEqual(3, res.response[0].output)

  def test_computation(self):
    inputs = ["a", "bb", "ccc", "aa", "c", "bbb", "aa"]
    with RPCServer(1234), beam.Pipeline(
        options=PipelineOptions(["--streaming", "--runner=flink"])) as p:
      res = p | beam.Create(inputs) | RPCParDoStateful("localhost:1234", 4, 500)
      assert_that(res, equal_to([(x, len(x)) for x in inputs]))

if __name__ == '__main__':
    unittest.main()
