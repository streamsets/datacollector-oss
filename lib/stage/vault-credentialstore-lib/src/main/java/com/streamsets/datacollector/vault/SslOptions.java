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

public class SslOptions {
  private final String enabledProtocols;
  private final String trustStoreFile;
  private final String trustStorePassword;
  private final boolean sslVerify;
  private final int sslTimeout;

  public SslOptions(
      String enabledProtocols,
      String trustStoreFile,
      String trustStorePassword,
      boolean sslVerify,
      int sslTimeout
  ) {
    this.enabledProtocols = enabledProtocols;
    this.trustStoreFile = trustStoreFile;
    this.trustStorePassword = trustStorePassword;
    this.sslVerify = sslVerify;
    this.sslTimeout = sslTimeout;
  }

  public String getEnabledProtocols() {
    return enabledProtocols;
  }

  public String getTrustStoreFile() {
    return trustStoreFile;
  }

  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  public boolean isSslVerify() {
    return sslVerify;
  }

  public int getSslTimeout() {
    return sslTimeout;
  }
}
