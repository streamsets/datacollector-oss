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
package com.streamsets.datacollector.event.client.impl;

import com.streamsets.datacollector.event.client.api.EventClient;
import com.streamsets.datacollector.event.client.api.EventException;
import com.streamsets.datacollector.event.dto.Event;
import com.streamsets.datacollector.event.json.ClientEventJson;
import com.streamsets.datacollector.event.json.SDCMetricsJson;
import com.streamsets.datacollector.event.json.ServerEventJson;
import com.streamsets.lib.security.http.DpmClientInfo;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.pipeline.api.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;
import org.glassfish.jersey.client.filter.EncodingFilter;
import org.glassfish.jersey.message.GZipEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class EventClientImpl implements EventClient {
  private static final Logger LOG = LoggerFactory.getLogger(EventClientImpl.class);

  private final Configuration conf;
  private final Supplier<DpmClientInfo> dpmClientInfoSupplier;
  public static final String EVENT_CONNECT_TIMEOUT = "event.connect.timeout";
  public static final String EVENT_READ_TIMEOUT = "event.read.timeout";
  public static final int EVENT_CONNECT_TIMEOUT_DEFAULT = 10000;
  // Change the default read timeout from 10 seconds to 60 seconds. This is consistent with SCH's default query timeout
  // to be 60 seconds
  public static final int EVENT_READ_TIMEOUT_DEFAULT = 60000;

  public EventClientImpl(Configuration conf, Supplier<DpmClientInfo> clientInfoSupplier) {
    this.conf = conf;
    this.dpmClientInfoSupplier = clientInfoSupplier;
  }

  private Client getRestClient() {
    DpmClientInfo clientInfo = dpmClientInfoSupplier.get();
    ClientConfig clientConfig = new ClientConfig()
        .property(ClientProperties.CONNECT_TIMEOUT, conf.get(EVENT_CONNECT_TIMEOUT, EVENT_CONNECT_TIMEOUT_DEFAULT))
        .property(ClientProperties.READ_TIMEOUT, conf.get(EVENT_READ_TIMEOUT, EVENT_READ_TIMEOUT_DEFAULT));
    clientConfig.register(new MovedDpmJerseyClientFilter(clientInfo));
    return ClientBuilder.newClient(clientConfig);

  }

  @Override
  public List<ServerEventJson> submit(
    String path,
    Map<String, String> queryParams,
    Map<String, String> headerParams,
    boolean compression,
    List<ClientEventJson> clientEventJson) throws EventException {
    Client client = getRestClient();
    if (compression) {
      client.register(GZipEncoder.class);
      client.register(EncodingFilter.class);
    }
    WebTarget target = client.target(dpmClientInfoSupplier.get().getDpmBaseUrl() + path);

    for (Map.Entry<String, String> entry : queryParams.entrySet()) {
      target = target.queryParam(entry.getKey(), entry.getValue());
    }

    Invocation.Builder builder = target.request();

    for (Map.Entry<String, String> entry : headerParams.entrySet()) {
      builder = builder.header(entry.getKey(), removeNewLine(entry.getValue()));
    }
    builder.header(SSOConstants.X_REST_CALL, SSOConstants.SDC_COMPONENT_NAME);
    for (Map.Entry<String, String> entry : dpmClientInfoSupplier.get().getHeaders().entrySet()) {
      builder = builder.header(entry.getKey(), removeNewLine(entry.getValue()));
    }

    Response response = null;
    try {
      response = builder.post(Entity.json(clientEventJson));
      if (response.getStatus() != 200) {
        throw new EventException(Utils.format("Failed : {} HTTP error code : {}", path, response.getStatus()));
      }
      return response.readEntity(new GenericType<List<ServerEventJson>>() {
      });
    } catch (Exception ex) {
      throw new EventException(Utils.format("Failed to read response for {} : {}", path, ex));
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  private void _submitSync(
      String path,
      Map<String, String> queryParams,
      Map<String, String> headerParams,
      Object entity,
      long retryAttempts
  ) {
    WebTarget webTarget = getRestClient().target(dpmClientInfoSupplier.get().getDpmBaseUrl() + path);
    int delaySecs = 1;
    int attempts = 0;
    while (attempts < retryAttempts || retryAttempts == -1) {
      if (attempts > 0) {
        delaySecs = delaySecs * 2;
        delaySecs = Math.min(delaySecs, 60);
        LOG.warn("Post attempt '{}', waiting for '{}' seconds before retrying sending sync evens ...",
            attempts, delaySecs);
        sleep(delaySecs);
      }
      attempts++;
      Response response = null;
      try {
        for (Map.Entry<String, String> entry : queryParams.entrySet()) {
          webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
        }
        Invocation.Builder builder = webTarget.request();
        for (Map.Entry<String, String> entry : headerParams.entrySet()) {
          builder = builder.header(entry.getKey(), removeNewLine(entry.getValue()));
        }
        builder.header(SSOConstants.X_REST_CALL, SSOConstants.SDC_COMPONENT_NAME);
        for (Map.Entry<String, String> entry : dpmClientInfoSupplier.get().getHeaders().entrySet()) {
          builder = builder.header(entry.getKey(), removeNewLine(entry.getValue()));
        }
        response = entity != null ? builder.post(Entity.json(entity)) : builder.post(null);
        if (response.getStatus() == HttpURLConnection.HTTP_OK) {
          return;
        } else if (response.getStatus() == HttpURLConnection.HTTP_UNAVAILABLE) {
          LOG.warn("Error writing DPM, Service unavailable");
          // retry
        } else {
          String responseMessage = response.readEntity(String.class);
          LOG.error(Utils.format("Error writing to DPM: {}, status code: {}", responseMessage, response.getStatus()));
          //retry
        }
      } catch (Exception ex) {
        LOG.error(Utils.format("Error writing to DPM: {}", ex.toString()), ex);
        // retry
      } finally {
        if (response != null) {
          response.close();
        }
      }
    }

    // no success after retry
    LOG.error("Unable to write events to DPM after {} attempts", retryAttempts);
  }

  @Override
  public void submit(
      String path,
      Map<String, String> queryParams,
      Map<String, String> headerParams,
      List<SDCMetricsJson> sdcMetricsJsons,
      long retryAttempts
  ) {
    _submitSync(path, queryParams, headerParams, sdcMetricsJsons, retryAttempts);
  }

  @Override
  public void sendSyncEvents(
      String path,
      Map<String, String> queryParams,
      Map<String, String> headerParams,
      Event event,
      long retryAttempts) {
    _submitSync(path, queryParams, headerParams, event, retryAttempts);
  }

  private static void sleep(int secs) {
    try {
      Thread.sleep(secs * 1000);
    } catch (InterruptedException ex) {
      String msg = "Interrupted while attempting to send events to DPM";
      LOG.error(msg);
      throw new RuntimeException(msg, ex);
    }
  }

  public String removeNewLine(String original) {
    return original.replaceAll("(\\r|\\n)", "");
  }
}
