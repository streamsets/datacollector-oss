/*
 * Copyright 2019 StreamSets Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.datacollector.thycotic;

import com.streamsets.datacollector.thycotic.SslOptions;

public class ThycoticConfiguration {

  private final String address;
  private final int openTimeout;
  private final int readTimeout;
  private final SslOptions sslOptions;
  private final int timeout;

  public ThycoticConfiguration(
      String address, int openTimeout, int readTimeout, SslOptions sslOptions, int timeout
  ) {
    this.address = address;
    this.openTimeout = openTimeout;
    this.readTimeout = readTimeout;
    this.sslOptions = sslOptions;
    this.timeout = timeout;
  }

  public String getAddress() {
    return address;
  }

  public int getOpenTimeout() {
    return openTimeout;
  }

  public int getReadTimeout() {
    return readTimeout;
  }

  public SslOptions getSslOptions() {
    return sslOptions;
  }

  public int getTimeout() {
    return timeout;
  }
}
