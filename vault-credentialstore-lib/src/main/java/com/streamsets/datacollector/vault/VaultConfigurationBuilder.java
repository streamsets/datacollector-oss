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

public final class VaultConfigurationBuilder {
  public static final String DEFAULT_ADDRESS = "http://localhost:8200";

  private String address = DEFAULT_ADDRESS;
  private String token = "";
  private int openTimeout = 0;
  private ProxyOptions proxyOptions = ProxyOptionsBuilder.newProxyOptions().build();
  private int readTimeout = 0;
  private SslOptions sslOptions = SslOptionsBuilder.newSslOptions().build();
  private int timeout = 0;

  private VaultConfigurationBuilder() {
  }

  public static VaultConfigurationBuilder newVaultConfiguration() {
    return new VaultConfigurationBuilder();
  }

  public VaultConfigurationBuilder fromVaultConfiguration(VaultConfiguration conf) {
    return new VaultConfigurationBuilder()
        .withAddress(conf.getAddress())
        .withToken(conf.getToken())
        .withOpenTimeout(conf.getOpenTimeout())
        .withProxyOptions(conf.getProxyOptions())
        .withReadTimeout(conf.getReadTimeout())
        .withSslOptions(conf.getSslOptions())
        .withTimeout(conf.getTimeout());
  }

  public VaultConfigurationBuilder withAddress(String address) {
    this.address = address;
    return this;
  }

  public VaultConfigurationBuilder withToken(String token) {
    this.token = token;
    return this;
  }

  public VaultConfigurationBuilder withOpenTimeout(int openTimeout) {
    this.openTimeout = openTimeout;
    return this;
  }

  public VaultConfigurationBuilder withProxyOptions(ProxyOptions proxyOptions) {
    this.proxyOptions = proxyOptions;
    return this;
  }

  public VaultConfigurationBuilder withReadTimeout(int readTimeout) {
    this.readTimeout = readTimeout;
    return this;
  }

  public VaultConfigurationBuilder withSslOptions(SslOptions sslOptions) {
    this.sslOptions = sslOptions;
    return this;
  }

  public VaultConfigurationBuilder withTimeout(int timeout) {
    this.timeout = timeout;
    return this;
  }

  public VaultConfiguration build() {
    return new VaultConfiguration(address, token, openTimeout, proxyOptions, readTimeout, sslOptions, timeout);
  }
}
