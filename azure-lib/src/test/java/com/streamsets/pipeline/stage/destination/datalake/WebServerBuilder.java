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
package com.streamsets.pipeline.stage.destination.datalake;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.QueueDispatcher;

import java.io.IOException;

public class WebServerBuilder {
  private MockWebServer server;

  public WebServerBuilder() {
    server = new MockWebServer();
    QueueDispatcher dispatcher = new QueueDispatcher();
    dispatcher.setFailFast(new MockResponse().setResponseCode(400));
    server.setDispatcher(dispatcher);
  }

  public String getAccountFQDN() {
    return server.getHostName() + ":" + server.getPort();
  }

  public String getAuthTokenEndPoint() {
    return "http://" + server.getHostName() + ":" + server.getPort();
  }

  public void shutdown() throws IOException {
    server.shutdown();
  }

  public WebServerBuilder enqueueTokenSuccess() {
    MockResponse tokenSuccessResponse = (new MockResponse()).setResponseCode(200);
    final String tokenSuccessResponseBody = "{\"access_token\":\"test\", \"expires_in\":123456789}";
    tokenSuccessResponse.setBody(tokenSuccessResponseBody);

    server.enqueue(tokenSuccessResponse);
    return this;
  }

  public WebServerBuilder enqueueClientSuccessResponse() {
    MockResponse clientSuccessResponse = (new MockResponse()).setResponseCode(200);
    final String clientSuccessBody = "{\"accessTime\":123456789, \"modificationTime\":123456789}";
    clientSuccessResponse.setBody(clientSuccessBody);

    server.enqueue(clientSuccessResponse);
    return this;
  }

  public WebServerBuilder enqueueFileInfoSuccessResponse() {
    MockResponse fileInfoResponse = (new MockResponse()).setResponseCode(200);
    final String fileInfoResponseBody = "{\"length\":100, \"accessTime\":123456789, \"type\":\"FILE\", " +
        "\"modificationTime\":123456789, \"permission\":\"read\",\"owner\":\"root\", " +
        "\"group\":\"supergroup\",\"blockSize\":1,\"replication\":1}";
    fileInfoResponse.setBody(fileInfoResponseBody);

    server.enqueue(fileInfoResponse);
    return this;
  }

  public WebServerBuilder enqueueFileInfoFailureResponse() {
    MockResponse fileInfoFailureResponse = (new MockResponse()).setResponseCode(408); // client timeout
    final String fileInfoResponseBody = "{\"length\":100, \"accessTime\":123456789, \"type\":\"FILE\", " +
        "\"modificationTime\":123456789, \"permission\":\"read\",\"owner\":\"root\", " +
        "\"group\":\"supergroup\",\"blockSize\":1,\"replication\":1}";
    fileInfoFailureResponse.setBody(fileInfoResponseBody);

    server.enqueue(fileInfoFailureResponse);

    return this;
  }

  public WebServerBuilder enqueueTokenFailureResponse() {
    MockResponse fileInfoFailureResponse = (new MockResponse()).setResponseCode(401); // client timeout
    final String fileInfoResponseBody = "{\"length\":100, \"accessTime\":123456789, \"type\":\"FILE\", " +
        "\"modificationTime\":123456789, \"permission\":\"read\",\"owner\":\"root\", " +
        "\"group\":\"supergroup\",\"blockSize\":1,\"replication\":1}";
    fileInfoFailureResponse.setBody(fileInfoResponseBody);

    server.enqueue(fileInfoFailureResponse);

    return this;
  }
}
