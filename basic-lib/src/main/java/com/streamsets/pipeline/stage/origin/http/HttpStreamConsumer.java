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
import com.streamsets.datacollector.el.VaultEL;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.http.AuthenticationType;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.http.JerseyClientUtil;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ChunkedInput;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.oauth1.AccessToken;
import org.glassfish.jersey.client.oauth1.OAuth1ClientSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.AsyncInvoker;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
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
  private static final String RESOURCE_CONFIG_NAME = "resourceUrl";
  private static final String REQUEST_BODY_CONFIG_NAME = "requestData";
  private static final String HEADER_CONFIG_NAME = "headers";
  private static final String VAULT_EL_PREFIX = "${" + VaultEL.PREFIX;

  private final HttpClientConfigBean conf;
  private final Client client;
  private final BlockingQueue<String> entityQueue;

  private ELVars resourceVars;
  private ELVars bodyVars;
  private ELVars headerVars;
  private ELEval resourceEval;
  private ELEval bodyEval;
  private ELEval headerEval;

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
  public HttpStreamConsumer(HttpClientConfigBean conf, Source.Context context, BlockingQueue<String> entityQueue) {
    this.conf = conf;
    this.entityQueue = entityQueue;

    resourceVars = context.createELVars();
    resourceEval = context.createELEval(RESOURCE_CONFIG_NAME);

    bodyVars = context.createELVars();
    bodyEval = context.createELEval(REQUEST_BODY_CONFIG_NAME);

    headerVars = context.createELVars();
    headerEval = context.createELEval(HEADER_CONFIG_NAME);

    ClientConfig clientConfig = new ClientConfig().connectorProvider(new ApacheConnectorProvider());

    ClientBuilder clientBuilder = ClientBuilder.newBuilder().withConfig(clientConfig);

    configureAuth(clientBuilder);

    if (conf.useProxy) {
      JerseyClientUtil.configureProxy(conf.proxy, clientBuilder);
    }

    JerseyClientUtil.configureSslContext(conf.sslConfig, clientBuilder);

    client = clientBuilder.build();
  }

  private void configureAuth(ClientBuilder clientBuilder) {
    if (conf.authType == AuthenticationType.OAUTH) {
      authToken = JerseyClientUtil.configureOAuth1(conf.oauth, clientBuilder);
    } else if (conf.authType != AuthenticationType.NONE) {
      JerseyClientUtil.configurePasswordAuth(conf.authType, conf.basicAuth, clientBuilder);
    }
  }

  @Override
  public void run() {
    try {
      String resolvedUrl = resourceEval.eval(resourceVars, conf.resourceUrl, String.class);

      WebTarget resource = client.target(resolvedUrl);

      // If the request (headers or body) contain a known sensitive EL and we're not using https then fail the request.
      if (requestContainsSensitiveInfo() && !resource.getUri().getScheme().toLowerCase().startsWith("https")) {
        error = Optional.of((Exception) new StageException(Errors.HTTP_07));
        return;
      }

      for (Map.Entry<String, Object> entry : resource.getConfiguration().getProperties().entrySet()) {
        LOG.debug("Config: {}, {}", entry.getKey(), entry.getValue());
      }

      final AsyncInvoker asyncInvoker = resource.request()
          .property(OAuth1ClientSupport.OAUTH_PROPERTY_ACCESS_TOKEN, authToken)
          .headers(resolveHeaders())
          .async();
      Future<Response> responseFuture;
      if (conf.requestData != null && !conf.requestData.isEmpty() && conf.httpMethod != HttpMethod.GET) {
        final String requestBody = bodyEval.eval(bodyVars, conf.requestData, String.class);

        responseFuture = asyncInvoker.method(conf.httpMethod.getLabel(), Entity.json(requestBody));
      } else {
        responseFuture = asyncInvoker.method(conf.httpMethod.getLabel());
      }
      Response response = responseFuture.get(conf.requestTimeoutMillis, TimeUnit.MILLISECONDS);
      lastResponseStatus = response.getStatusInfo();

      final ChunkedInput<String> chunkedInput = response.readEntity(new GenericType<ChunkedInput<String>>() {
      });
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
        if (conf.httpMode == HttpClientMode.POLLING) {
          // close response only in polling mode.
          // Closing response in streaming mode may fail with ApacheConnectorProvider
          // We close client in streaming mode which will close associated resources
          response.close();
        }
      }
      LOG.debug("HTTP stream consumer closed.");
    } catch (ELEvalException e) {
      LOG.error(Errors.HTTP_06.getMessage(), e.toString(), e);
      error = Optional.of((Exception) new StageException(Errors.HTTP_06, e.toString(), e));
    } catch (InterruptedException | ExecutionException e) {
      LOG.warn(Errors.HTTP_01.getMessage(), e.toString(), e);
      error = Optional.of((Exception)e);
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      LOG.warn("HTTP request future timed out", e.toString(), e);
      error = Optional.of((Exception)e);
      Thread.currentThread().interrupt();
    } finally {
      // close client in polling mode only on stop since we reuse the client
      if (conf.httpMode == HttpClientMode.POLLING) {
        if (stop) {
          client.close();
        }
      } else {
        // Always close client in streaming mode on completion
        client.close();
      }
    }
  }

  private boolean requestContainsSensitiveInfo() {
    boolean sensitive = false;
    for (Map.Entry<String, String> header : conf.headers.entrySet()) {
      if (header.getKey().contains(VAULT_EL_PREFIX) || header.getValue().contains(VAULT_EL_PREFIX)) {
        sensitive = true;
        break;
      }
    }

    if (conf.requestData != null && conf.requestData.contains(VAULT_EL_PREFIX)) {
      sensitive = true;
    }

    return sensitive;
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

  private MultivaluedMap<String, Object> resolveHeaders() {
    MultivaluedMap<String, Object> requestHeaders = new MultivaluedHashMap<>();
    for (Map.Entry<String, String> entry : conf.headers.entrySet()) {
      List<Object> header = new ArrayList<>(1);
      try {
        Object resolvedValue = headerEval.eval(headerVars, entry.getValue(), String.class);
        header.add(resolvedValue);
        requestHeaders.put(entry.getKey(), header);
      } catch (StageException e) {
        LOG.error("Failed to evaluate '{}' in header. {}", entry.getValue(), e.toString(), e);
        error = Optional.of((Exception)e);
      }
    }

    return requestHeaders;
  }
}
