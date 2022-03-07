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

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Server;
import lombok.Value;

@VisibleForTesting
@Value
class AutoCloseableServer implements AutoCloseable {
  static AutoCloseableServer of(Server server) {
    return new AutoCloseableServer(server);
  }

  Server server;

  @Override
  public void close() {
    server.shutdownNow();
  }
}
