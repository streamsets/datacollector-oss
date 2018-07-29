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
package com.streamsets.datacollector.vault;

public class VaultConfiguration {
  private final String address;
  private final String token;
  private final int openTimeout;
  private final ProxyOptions proxyOptions;
  private final int readTimeout;
  private final SslOptions sslOptions;
  private final int timeout;

  public VaultConfiguration(
      String address,
      String token,
      int openTimeout,
      ProxyOptions proxyOptions,
      int readTimeout,
      SslOptions sslOptions,
      int timeout
  ) {
    this.address = address;
    this.token = token;
    this.openTimeout = openTimeout;
    this.proxyOptions = proxyOptions;
    this.readTimeout = readTimeout;
    this.sslOptions = sslOptions;
    this.timeout = timeout;
  }

  public String getAddress() {
    return address;
  }

  public String getToken() {
    return token;
  }

  public int getOpenTimeout() {
    return openTimeout;
  }

  public ProxyOptions getProxyOptions() {
    return proxyOptions;
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
