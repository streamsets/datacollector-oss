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

public final class SslOptionsBuilder {
  static final String DEFAULT_PROTOCOLS = "TLSv1.2,TLSv1.3";

  private String enabledProtocols = DEFAULT_PROTOCOLS;
  private String trustStoreFile;
  private String trustStorePassword;
  private boolean sslVerify = true;
  private int sslTimeout = 0;

  private SslOptionsBuilder() {
  }

  public static SslOptionsBuilder newSslOptions() {
    return new SslOptionsBuilder();
  }

  public SslOptionsBuilder withEnabledProtocols(String enabledCiphers) {
    this.enabledProtocols = enabledCiphers;
    return this;
  }

  public SslOptionsBuilder withTrustStoreFile(String trustStoreFile) {
    this.trustStoreFile = trustStoreFile;
    return this;
  }

  public SslOptionsBuilder withTrustStorePassword(String trustStorePassword) {
    this.trustStorePassword = trustStorePassword;
    return this;
  }

  public SslOptionsBuilder withSslVerify(boolean sslVerify) {
    this.sslVerify = sslVerify;
    return this;
  }

  public SslOptionsBuilder withSslTimeout(int sslTimeout) {
    this.sslTimeout = sslTimeout;
    return this;
  }

  public SslOptions build() {
    return new SslOptions(enabledProtocols, trustStoreFile, trustStorePassword, sslVerify, sslTimeout);
  }
}
