/*
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.testing;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for network-related test setup.
 */
public class NetworkUtils {
  private NetworkUtils() {}

  /**
   * Create port number that is available on this machine for
   * subsequent use.
   *
   * Please note that this method doesn't guarantee that you
   * will be able to subsequently use that port as there is
   * a race condition when this method can return port number
   * that is available at the time of the call, but will be
   * used by different process before the caller will have a
   * chance to use it.
   *
   * This is merely a helper method to find a random available
   * port on the machine to enable multiple text executions
   * on the same machine or deal with situation when some tools
   * (such as Derby) do need specific port and can't be started
   * on "random" port.
   *
   * @return Available port number
   */
  public static int getRandomPort() throws IOException {
    try (ServerSocket ss = new ServerSocket(0)) {
      return ss.getLocalPort();
    }
  }

  /**
   * Create a list of currently available port numbers as strings.
   *
   * This simply calls {@link NetworkUtils#getRandomPort()} n times to build a list.
   * The same caveats apply as not all ports may still be available upon attempted use.
   *
   * @param n number of ports to find
   * @return list of available ports
   * @throws IOException
   */
  public static List<String> getRandomPorts(int n) throws IOException {
    List<String> ports = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      ports.add(String.valueOf(getRandomPort()));
    }
    return ports;
  }
}
