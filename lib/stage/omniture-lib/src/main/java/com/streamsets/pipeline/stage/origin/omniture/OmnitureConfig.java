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
package com.streamsets.pipeline.stage.origin.omniture;

import com.streamsets.pipeline.api.credential.CredentialValue;

public class OmnitureConfig {

  // The HTTP client mode (only polling for now)
  HttpClientMode httpMode = null;

  // URL of the streaming JSON resource to ingest.
  String resourceUrl = null;

  // timeout used for http requests and buffering.
  long requestTimeoutMillis = 10000;

  // Maximum records to queue before sending downstream.
  int batchSize = 1;

  // Maximum time to wait before sending a batch regardless of size.
  long maxBatchWaitTime = 10000;

  // Interval of time in milliseconds before another poll request is issued
  long pollingInterval = 60000;

  // Username for Omniture APIs
  CredentialValue username;

  // Shared secret for Omniture APIs
  CredentialValue sharedSecret;

  // JSON report description to define the request
  String reportDescription;

  // Proxy settings for the HTTP connection
  HttpProxyConfigBean proxySettings = null;

  public String getReportDescription() {
    return reportDescription;
  }

  public void setReportDescription(String reportDescription) {
    this.reportDescription = reportDescription;
  }

  public HttpClientMode getHttpMode() {
    return httpMode;
  }

  public void setHttpMode(HttpClientMode httpMode) {
    this.httpMode = httpMode;
  }

  public String getResourceUrl() {
    return resourceUrl;
  }

  public void setResourceUrl(String resourceUrl) {
    this.resourceUrl = resourceUrl;
  }

  public long getRequestTimeoutMillis() {
    return requestTimeoutMillis;
  }

  public void setRequestTimeoutMillis(long requestTimeoutMillis) {
    this.requestTimeoutMillis = requestTimeoutMillis;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public long getMaxBatchWaitTime() {
    return maxBatchWaitTime;
  }

  public void setMaxBatchWaitTime(long maxBatchWaitTime) {
    this.maxBatchWaitTime = maxBatchWaitTime;
  }

  public long getPollingInterval() {
    return pollingInterval;
  }

  public void setPollingInterval(long pollingInterval) {
    this.pollingInterval = pollingInterval;
  }

  public CredentialValue getUsername() {
    return username;
  }

  public void setUsername(CredentialValue username) {
    this.username = username;
  }

  public CredentialValue getSharedSecret() {
    return sharedSecret;
  }

  public void setSharedSecret(CredentialValue sharedSecret) {
    this.sharedSecret = sharedSecret;
  }

  public void setProxySettings(HttpProxyConfigBean proxySettings) { this.proxySettings = proxySettings; }

  public HttpProxyConfigBean getProxySettings() { return proxySettings; }
}
