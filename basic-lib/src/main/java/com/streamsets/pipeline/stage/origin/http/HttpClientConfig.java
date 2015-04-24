/*
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.pipeline.stage.origin.http;

public class HttpClientConfig {
  // The HTTP client mode (streaming or polling)
  private HttpClientMode httpMode = null;

  // URL of the streaming JSON resource to ingest.
  private String resourceUrl = null;

  // timeout used for http requests and buffering.
  private long requestTimeoutMillis = 1000;

  // String delimiter between records in the stream.
  private String entityDelimiter = null;

  // Maximum records to queue before sending downstream.
  private int batchSize = 1000;

  // Maximum time to wait before sending a batch regardless of size.
  private long maxBatchWaitTime = 5000;

  // Interval of time in milliseconds before another poll request is issued
  private long pollingInterval = 5000;

  // HTTP method to request (GET, POST, etc.)
  private HttpMethod httpMethod = null;

  // Data to include with request (not used with GET or HEAD)
  private String requestData = null;

  // OAuth required parameters
  private String consumerKey = null;
  private String consumerSecret = null;
  private String token = null;
  private String tokenSecret = null;


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

  public String getEntityDelimiter() {
    return entityDelimiter;
  }

  public void setEntityDelimiter(String entityDelimiter) {
    this.entityDelimiter = entityDelimiter;
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

  public HttpMethod getHttpMethod() {
    return httpMethod;
  }

  public void setHttpMethod(HttpMethod httpMethod) {
    this.httpMethod = httpMethod;
  }

  public String getRequestData() {
    return requestData;
  }

  public void setRequestData(String requestData) {
    this.requestData = requestData;
  }

  public String getConsumerKey() {
    return consumerKey;
  }

  public void setConsumerKey(String consumerKey) {
    this.consumerKey = consumerKey;
  }

  public String getConsumerSecret() {
    return consumerSecret;
  }

  public void setConsumerSecret(String consumerSecret) {
    this.consumerSecret = consumerSecret;
  }

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  public String getTokenSecret() {
    return tokenSecret;
  }

  public void setTokenSecret(String tokenSecret) {
    this.tokenSecret = tokenSecret;
  }
}
