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

public final class ProxyOptionsBuilder {
  private String address;
  private int port = 8080;
  private String username;
  private String password;

  private ProxyOptionsBuilder() {
  }

  public static ProxyOptionsBuilder newProxyOptions() {
    return new ProxyOptionsBuilder();
  }

  public ProxyOptionsBuilder withProxyAddress(String proxyAddress) {
    this.address = proxyAddress;
    return this;
  }

  public ProxyOptionsBuilder withProxyPort(int proxyPort) {
    this.port = proxyPort;
    return this;
  }

  public ProxyOptionsBuilder withProxyUsername(String proxyUsername) {
    this.username = proxyUsername;
    return this;
  }

  public ProxyOptionsBuilder withProxyPassword(String proxyPassword) {
    this.password = proxyPassword;
    return this;
  }

  public ProxyOptions build() {
    return new ProxyOptions(address, port, username, password);
  }
}
