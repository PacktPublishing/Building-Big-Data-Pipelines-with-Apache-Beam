#!/usr/bin/env python3

import sys
import grpc
import socket
import apache_beam as beam
from apache_beam.transforms import DoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.kafka import WriteToKafka
from apache_beam.transforms.userstate import *

from beam_utils import get_expansion_service

from concurrent import futures
import service_pb2
import service_pb2_grpc

def usage():
  sys.stderr.write(
      "Usage: %s <bootstrapServer> <inputTopic> <outputTopic> "
      "<batchSize> <maxWaitTime>\n"
      % (sys.argv[0], ))
  sys.exit(1)

class RPCServer(service_pb2_grpc.RpcServiceServicer):

  server = None
  port = None

  def __init__(self, port=None):
    self.port = port

  def __enter__(self):
    if not self.port:
      raise ValueError("Missing port in constructor")
    self.start(self.port)

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.stop()

  def resolve(self, request, context):
    raise Exception("Unimplemented. Use resolveBatch.")

  def resolveBatch(self, request, context):
    res = service_pb2.ResponseList()
    res.response.extend([service_pb2.Response(output=len(r.input)) for r in request.request])
    return res

  def start(self, port):
    self.server = grpc.server(futures.ThreadPoolExecutor())
    service_pb2_grpc.add_RpcServiceServicer_to_server(self, self.server)
    self.server.add_insecure_port("0:%d" % (int(port), ))
    self.server.start()

  def stop(self):
    self.server.stop(True)
    self.server.wait_for_termination()


class RPCDoFn(DoFn):

  channel = None
  stub = None

  BATCH_SIZE = ReadModifyWriteStateSpec("batchSize", beam.coders.VarIntCoder())
  BATCH = BagStateSpec("batch", beam.coders.StrUtf8Coder())
  FLUSH_TIMER = TimerSpec("flushTimer", TimeDomain.REAL_TIME)
  EOW_TIMER = TimerSpec("endOfTime", TimeDomain.WATERMARK)

  def __init__(self, address, batchSize, maxWaitTime):
    self.address = address
    self.batchSize = batchSize
    self.maxWaitTime = maxWaitTime

  def setup(self):
    import grpc
    import service_pb2_grpc

    self.channel = grpc.insecure_channel(self.address)
    self.stub = service_pb2_grpc.RpcServiceStub(self.channel)

  def teardown(self):
    self.channel.close()

  def process(
      self,
      element,
      batch=DoFn.StateParam(BATCH),
      batchSize=DoFn.StateParam(BATCH_SIZE),
      flushTimer=DoFn.TimerParam(FLUSH_TIMER),
      endOfTime=DoFn.TimerParam(EOW_TIMER)):

    from apache_beam.utils.timestamp import Timestamp, Duration
    from apache_beam.transforms.window import GlobalWindow

    currentSize = batchSize.read()
    if not currentSize:
      currentSize = 1
      flushTimer.set(Timestamp.now() + Duration(micros=self.maxWaitTime * 1000))
      endOfTime.set(GlobalWindow().max_timestamp())
    else:
      currentSize += 1
    batchSize.write(currentSize)
    batch.add(element[1])
    if currentSize >= self.batchSize:
      return self.flush(batch, batchSize)

  @on_timer(FLUSH_TIMER)
  def onFlushTimer(
      self,
      batch = DoFn.StateParam(BATCH),
      batchSize = DoFn.StateParam(BATCH_SIZE)):

    return self.flush(batch, batchSize)

  @on_timer(EOW_TIMER)
  def onEndOfTime(
      self,
      batch = DoFn.StateParam(BATCH),
      batchSize = DoFn.StateParam(BATCH_SIZE)):

    return self.flush(batch, batchSize)

  def flush(self, batch, batchSize):

    import service_pb2

    batchSize.clear()
    req = service_pb2.RequestList()
    inputs = batch.read()
    req.request.extend([service_pb2.Request(input=item) for item in set(inputs)])
    batch.clear()
    res = self.stub.resolveBatch(req)
    resolved = {req.request[i].input: res.response[i].output for i in range(0, len(req.request))}
    return [(elem, resolved[elem]) for elem in inputs]

def toBuckets(x: str) -> beam.typehints.KV[int, str]:
  return (ord(x[0]) % 10, x)

@beam.ptransform_fn
def RPCParDoStateful(input, address="localhost:1234", batchSize=10, maxWaitTime=None):
  return (input
          | beam.Map(toBuckets)
          | beam.ParDo(RPCDoFn(address, batchSize, maxWaitTime)))

def toKv(t: beam.typehints.Tuple[str, int]) -> beam.typehints.KV[str, str]:
  return ("", "%s:%d" % t)

if __name__ == "__main__":
  if len(sys.argv) < 6:
    usage()

  bootstrapServer, inputTopic, outputTopic = sys.argv[1:4]
  batchSize, maxWaitTime = map(int, sys.argv[4:6])

  # pod hostnames are not resolvable in K8s, we need IP
  local_ip = socket.gethostbyname(socket.gethostname())
  port = 1234

  with RPCServer(port):
    with beam.Pipeline(options=PipelineOptions(["--streaming"] + sys.argv[6:])) as p:
      (p | ReadFromKafka(
          consumer_config={'bootstrap.servers': bootstrapServer},
          topics=[inputTopic],
          expansion_service=get_expansion_service())
       | beam.Map(lambda x: x[1].decode("utf-8"))
       | "RPCParDo" >> RPCParDoStateful("%s:%d" % (local_ip, port), batchSize, maxWaitTime)
       | "Format" >> beam.Map(toKv)
       | "StoreOutput" >> WriteToKafka(
           producer_config={'bootstrap.servers': bootstrapServer},
           topic=outputTopic,
           key_serializer="org.apache.kafka.common.serialization.StringSerializer",
           value_serializer="org.apache.kafka.common.serialization.StringSerializer",
           expansion_service=get_expansion_service()))

