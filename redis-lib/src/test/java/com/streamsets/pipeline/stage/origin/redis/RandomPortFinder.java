/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.redis;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.concurrent.ThreadLocalRandom;

public class RandomPortFinder {

  private static final int MIN_PORT_NUMBER = 49152;
  private static final int MAX_PORT_NUMBER = 65535;


  public static int find() {
    while (true) {
      int port = ThreadLocalRandom.current().nextInt(MIN_PORT_NUMBER, MAX_PORT_NUMBER);
      if (available(port)) {
        return port;
      }
    }
  }

  private static boolean available(final int port) {
    ServerSocket serverSocket = null;
    DatagramSocket dataSocket = null;
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);
      dataSocket = new DatagramSocket(port);
      dataSocket.setReuseAddress(true);
      return true;
    } catch (final IOException e) {
      return false;
    } finally {
      if (dataSocket != null) {
        dataSocket.close();
      }
      if (serverSocket != null) {
        try {
          serverSocket.close();
        } catch (final IOException e) {

        }
      }
    }
  }
}
