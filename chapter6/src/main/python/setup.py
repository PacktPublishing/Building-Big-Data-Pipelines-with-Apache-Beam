import os
import sys
import grpc_tools.protoc as protoc
from setuptools import setup

protoDir = os.path.split(os.path.abspath(sys.argv[0]))[0] + "/../proto/"
proto = protoDir + "service.proto"

if os.path.exists(protoDir):
  protoc.main([
    "grpc_tools.protoc",
    "--proto_path=%s" % (protoDir, ),
    "--python_out=.",
    "--grpc_python_out=.",
    proto])

setup(name='packt_beam_chapter6',
  version='1.0',
  description='Packt Building Big Data Pipelines using Apache Beam chapter 6',
  py_modules=['service_pb2', 'service_pb2_grpc', 'utils'])


