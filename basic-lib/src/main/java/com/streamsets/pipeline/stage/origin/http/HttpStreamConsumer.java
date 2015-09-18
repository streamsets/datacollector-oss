/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.http;

import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.client.ChunkedInput;
import org.glassfish.jersey.client.oauth1.AccessToken;
import org.glassfish.jersey.client.oauth1.ConsumerCredentials;
import org.glassfish.jersey.client.oauth1.OAuth1ClientSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.AsyncInvoker;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Consumes the HTTP stream chunk by chunk buffering it into a queue for consumption
 * by the origin.
 */
class HttpStreamConsumer implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(HttpStreamConsumer.class);
  private WebTarget resource;
  private String httpMethod;
  private String requestData;
  private AccessToken authToken;

  private final long responseTimeoutMillis;
  private final String entityDelimiter;
  private final BlockingQueue<String> entityQueue;
  private int lastResponseStatus;

  private volatile boolean stop = false;

  /**
   * Constructor for unauthenticated connections.
   * @param resourceUrl URL of streaming JSON resource.
   * @param responseTimeoutMillis How long to wait for a response from http endpoint.
   * @param httpMethod HTTP method to send
   * @param requestData Data to be sent as a part of the request
   * @param entityDelimiter String delimiter that marks the end of a record.
   * @param entityQueue A queue to place received chunks (usually a single JSON object) into.
   */
  public HttpStreamConsumer(
      final String resourceUrl,
      final long responseTimeoutMillis,
      final String httpMethod,
      final String requestData,
      final String entityDelimiter,
      BlockingQueue<String> entityQueue
  ) {
    this.responseTimeoutMillis = responseTimeoutMillis;
    this.httpMethod = httpMethod;
    this.requestData = requestData;
    this.entityDelimiter = entityDelimiter;
    this.entityQueue = entityQueue;
    Client client = ClientBuilder.newClient();
    resource = client.target(resourceUrl);
  }

  /**
   * Constructor for OAuth authenticated connections. Requires a stored auth token since we
   * cannot display an authentication web page to confirm authorization at this time.
   * @param resourceUrl URL of streaming JSON resource.
   * @param responseTimeoutMillis How long to wait for a response from http endpoint.
   * @param httpMethod HTTP method to send
   * @param requestData Data to be sent as a part of the request
   * @param entityDelimiter String delimiter that marks the end of a record.
   * @param entityQueue A queue to place received chunks (usually a single JSON object) into.
   * @param consumerKey OAuth required parameter.
   * @param consumerSecret OAuth required parameter.
   * @param token OAuth required parameter.
   * @param tokenSecret OAuth required parameter.
   */
  public HttpStreamConsumer(
      final String resourceUrl,
      final long responseTimeoutMillis,
      final String httpMethod,
      final String requestData,
      final String entityDelimiter,
      BlockingQueue<String> entityQueue,
      final String consumerKey,
      final String consumerSecret,
      final String token,
      final String tokenSecret
  ) {
    this.responseTimeoutMillis = responseTimeoutMillis;
    this.httpMethod = httpMethod;
    this.requestData = requestData;
    this.entityDelimiter = entityDelimiter;
    this.entityQueue = entityQueue;

    ConsumerCredentials consumerCredentials = new ConsumerCredentials(
        consumerKey,
        consumerSecret
    );

    authToken = new AccessToken(token, tokenSecret);
    Feature feature = OAuth1ClientSupport.builder(consumerCredentials)
        .feature()
        .accessToken(authToken)
        .build();

    Client client = ClientBuilder.newBuilder()
        .register(feature)
        .build();
    resource = client.target(resourceUrl);
  }

  @Override
  public void run() {
    final AsyncInvoker asyncInvoker = resource.request()
        .property(OAuth1ClientSupport.OAUTH_PROPERTY_ACCESS_TOKEN, authToken)
        .async();
    final Future<Response> responseFuture;
    if (requestData != null && !requestData.isEmpty()) {
      responseFuture = asyncInvoker.method(httpMethod, Entity.json(requestData));
    } else{
      responseFuture = asyncInvoker.method(httpMethod);
    }
    Response response;
    try {
      response = responseFuture.get(responseTimeoutMillis, TimeUnit.MILLISECONDS);
      lastResponseStatus = response.getStatus();

      final ChunkedInput<String> chunkedInput = response.readEntity(new GenericType<ChunkedInput<String>>() {});
      chunkedInput.setParser(ChunkedInput.createParser(entityDelimiter));
      String chunk;
      try {
        while (!stop && (chunk = chunkedInput.read()) != null) {
          boolean accepted = entityQueue.offer(chunk, responseTimeoutMillis, TimeUnit.MILLISECONDS);
          if (!accepted) {
            LOG.warn("Response buffer full, dropped record.");
          }
        }
      } finally {
        chunkedInput.close();
        response.close();
      }
      LOG.debug("HTTP stream consumer closed.");
    } catch (InterruptedException | ExecutionException e) {
      LOG.warn(Errors.HTTP_01.getMessage(), e.toString(), e);
    } catch (TimeoutException e) {
      LOG.warn("HTTP request future timed out", e.toString(), e);
    }
  }

  public void stop() {
    stop = true;
  }

  public int getLastResponseStatus() {
    return lastResponseStatus;
  }
}