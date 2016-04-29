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

import com.google.common.base.Optional;
import org.glassfish.jersey.client.ChunkedInput;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.client.oauth1.AccessToken;
import org.glassfish.jersey.client.oauth1.ConsumerCredentials;
import org.glassfish.jersey.client.oauth1.OAuth1ClientSupport;
import org.glassfish.jersey.grizzly.connector.GrizzlyConnectorProvider;
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
import java.util.Map;
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

  private final HttpClientConfigBean conf;
  private final Client client;
  private final WebTarget resource;
  private final BlockingQueue<String> entityQueue;

  private AccessToken authToken;
  private Response.StatusType lastResponseStatus;
  private Optional<Exception> error = Optional.absent();

  private volatile boolean stop = false;

  /**
   * Constructor for unauthenticated connections.
   *
   * @param conf        The config bean containing all necessary configuration for consuming data.
   * @param entityQueue A queue to place received chunks (usually a single JSON object) into.
   */
  public HttpStreamConsumer(HttpClientConfigBean conf, BlockingQueue<String> entityQueue) {
    this.conf = conf;
    this.entityQueue = entityQueue;

    ClientConfig clientConfig = new ClientConfig()
        .connectorProvider(new GrizzlyConnectorProvider());

    ClientBuilder clientBuilder = ClientBuilder.newBuilder().withConfig(clientConfig);

    if (conf.authType == AuthenticationType.OAUTH) {
      ConsumerCredentials consumerCredentials = new ConsumerCredentials(
          conf.oauth.consumerKey,
          conf.oauth.consumerSecret
      );

      authToken = new AccessToken(conf.oauth.token, conf.oauth.tokenSecret);
      Feature feature = OAuth1ClientSupport.builder(consumerCredentials)
          .feature()
          .accessToken(authToken)
          .build();
      clientBuilder.register(feature);
    }

    if (conf.authType == AuthenticationType.BASIC) {
      clientBuilder.register(HttpAuthenticationFeature.universal(conf.basicAuth.username, conf.basicAuth.password));
    }

    configureProxy(clientBuilder);

    client = clientBuilder.build();
    resource = client.target(conf.resourceUrl);

    for (Map.Entry<String, Object> entry : resource.getConfiguration().getProperties().entrySet()) {
      LOG.info("Config: {}, {}", entry.getKey(), entry.getValue());
    }
  }

  private void configureProxy(ClientBuilder clientBuilder) {
    if (!conf.useProxy) {
      return;
    }

    if (!conf.proxy.uri.isEmpty()) {
      clientBuilder.property(ClientProperties.PROXY_URI, conf.proxy.uri);
      LOG.info("Using Proxy: '{}'", conf.proxy.uri);
    }
    if (!conf.proxy.username.isEmpty()) {
      clientBuilder.property(ClientProperties.PROXY_USERNAME, conf.proxy.username);
      LOG.info("Using Proxy Username: '{}'", conf.proxy.username);
    }
    if (!conf.proxy.password.isEmpty()) {
      clientBuilder.property(ClientProperties.PROXY_PASSWORD, conf.proxy.password);
      LOG.info("Using Proxy Password: '{}'", conf.proxy.password);
    }
  }

  @Override
  public void run() {
    final AsyncInvoker asyncInvoker = resource.request()
        .property(OAuth1ClientSupport.OAUTH_PROPERTY_ACCESS_TOKEN, authToken)
        .async();
    final Future<Response> responseFuture;
    if (conf.requestData != null && !conf.requestData.isEmpty() && conf.httpMethod != HttpMethod.GET) {
      responseFuture = asyncInvoker.method(conf.httpMethod.getLabel(), Entity.json(conf.requestData));
    } else {
      responseFuture = asyncInvoker.method(conf.httpMethod.getLabel());
    }
    Response response;
    try {
      response = responseFuture.get(conf.requestTimeoutMillis, TimeUnit.MILLISECONDS);
      lastResponseStatus = response.getStatusInfo();

      final ChunkedInput<String> chunkedInput = response.readEntity(
          new GenericType<ChunkedInput<String>>() {
          }
      );
      chunkedInput.setParser(ChunkedInput.createParser(conf.entityDelimiter));
      String chunk;
      try {
        while (!stop && (chunk = chunkedInput.read()) != null) {
          boolean accepted = entityQueue.offer(chunk, conf.requestTimeoutMillis, TimeUnit.MILLISECONDS);
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
      error = Optional.of((Exception)e);
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      LOG.warn("HTTP request future timed out", e.toString(), e);
      error = Optional.of((Exception)e);
      Thread.currentThread().interrupt();
    } finally {
      if (stop) {
        client.close();
      }
    }
  }

  public void stop() {
    stop = true;
  }

  public Response.StatusType getLastResponseStatus() {
    return lastResponseStatus;
  }

  public Optional<Exception> getError() {
    return error;
  }
}
