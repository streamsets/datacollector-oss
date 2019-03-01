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
import com.streamsets.datacollector.thycotic.SslOptionsBuilder;

public class ThycoticConfigurationBuilder {
  public static final String DEFAULT_ADDRESS = "http://localhost:18630";

  private String address = DEFAULT_ADDRESS;
  private int openTimeout = 0;
  private int readTimeout = 0;
  private SslOptions sslOptions = SslOptionsBuilder.newSslOptions().build();
  private int timeout = 0;

  private ThycoticConfigurationBuilder() {

  }

  public static ThycoticConfigurationBuilder newThycoticConfiguration() {
    return new ThycoticConfigurationBuilder();
  }

  public ThycoticConfigurationBuilder fromVaultConfiguration(ThycoticConfiguration conf) {
    return new ThycoticConfigurationBuilder().withAddress(conf.getAddress())
        .withOpenTimeout(conf.getOpenTimeout())
        .withReadTimeout(conf.getReadTimeout())
        .withSslOptions(conf.getSslOptions())
        .withTimeout(conf.getTimeout());
  }

  public ThycoticConfigurationBuilder withAddress(String address) {
    this.address = address;
    return this;
  }

  public ThycoticConfigurationBuilder withOpenTimeout(int openTimeout) {
    this.openTimeout = openTimeout;
    return this;
  }

  public ThycoticConfigurationBuilder withReadTimeout(int readTimeout) {
    this.readTimeout = readTimeout;
    return this;
  }

  public ThycoticConfigurationBuilder withSslOptions(SslOptions sslOptions) {
    this.sslOptions = sslOptions;
    return this;
  }

  public ThycoticConfigurationBuilder withTimeout(int timeout) {
    this.timeout = timeout;
    return this;
  }

  public ThycoticConfiguration build() {
    return new ThycoticConfiguration(address, openTimeout, readTimeout, sslOptions, timeout);
  }
}
