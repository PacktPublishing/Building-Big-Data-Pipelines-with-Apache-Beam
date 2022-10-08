FROM apache/beam_python3.7_sdk:2.33.0

RUN apt -y update
RUN python3 -m pip uninstall -y six  
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install python3-protobuf 
RUN python3 -m pip install six==1.15.0 
RUN python3 -m pip install grpcio==1.34.0 grpcio-tools
