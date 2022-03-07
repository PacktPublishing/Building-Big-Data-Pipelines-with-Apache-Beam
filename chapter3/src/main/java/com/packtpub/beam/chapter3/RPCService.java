/**
 * Copyright 2021-2022 Packt Publishing Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.packtpub.beam.chapter3;

import com.packtpub.beam.chapter3.Service.Request;
import com.packtpub.beam.chapter3.Service.RequestList;
import com.packtpub.beam.chapter3.Service.Response;
import com.packtpub.beam.chapter3.Service.ResponseList;
import com.packtpub.beam.chapter3.Service.ResponseList.Builder;
import io.grpc.stub.StreamObserver;

public class RPCService extends RpcServiceGrpc.RpcServiceImplBase {

  @Override
  public void resolve(Request request, StreamObserver<Response> responseObserver) {
    responseObserver.onNext(getResponseFor(request));
    responseObserver.onCompleted();
  }

  @Override
  public void resolveBatch(
      RequestList requestBatch, StreamObserver<ResponseList> responseObserver) {
    Builder builder = ResponseList.newBuilder();
    for (Request r : requestBatch.getRequestList()) {
      builder.addResponse(getResponseFor(r));
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  private Response getResponseFor(Request request) {
    return Response.newBuilder().setOutput(request.getInput().length()).build();
  }
}
