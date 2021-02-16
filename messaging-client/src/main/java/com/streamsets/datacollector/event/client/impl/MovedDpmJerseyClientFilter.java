/*
 * Copyright 2021 StreamSets Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.lib.security.http.DpmClientInfo;
import com.streamsets.lib.security.http.MovedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientResponseContext;
import javax.ws.rs.client.ClientResponseFilter;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Jersey client filter to handle a MOVED PERMANENTLY request updating the base new DPM URL.
 * <p/>
 * Idea taken from Jersey's {@code HttpAuthenticationFilter}
 */
public class MovedDpmJerseyClientFilter implements ClientResponseFilter {
  private static final Logger LOG = LoggerFactory.getLogger(MovedDpmJerseyClientFilter.class);

  public static final int HTTP_PERMANENT_REDIRECT_STATUS = 308; // reference: https://tools.ietf.org/html/rfc7538
  public static final String HTTP_LOCATION_HEADER = "Location";

  private final DpmClientInfo clientInfo;

  public MovedDpmJerseyClientFilter(DpmClientInfo clientInfo) {
    this.clientInfo = clientInfo;
  }

  @Override
  public void filter(ClientRequestContext request, ClientResponseContext response) throws IOException {
    if (response.getStatus() == HTTP_PERMANENT_REDIRECT_STATUS) {
      String newLocation = response.getHeaderString(HTTP_LOCATION_HEADER);
      LOG.info("Received a MOVED_PERMANENTLY to '{}'", newLocation);
      // storing new DPM base URL
      clientInfo.setDpmBaseUrl(MovedException.extractBaseUrl(newLocation));
      // repeating request
      repeatRequest(request, response);
    }
  }

  @VisibleForTesting
  Client createClient(ClientRequestContext request) {
    return ClientBuilder.newClient(request.getConfiguration());
  }

  @VisibleForTesting
  void repeatRequest(ClientRequestContext request, ClientResponseContext response) throws IOException {
    Client client = createClient(request);
    String method = request.getMethod();
    MediaType mediaType = request.getMediaType();
    URI lUri = request.getUri();
    String fullPath = lUri.toURL().getFile();
    if (fullPath.startsWith("/")) {
      // the DpmBaseUrl includes the first /
      fullPath = fullPath.substring(1);
    }
    WebTarget resourceTarget = client.target(clientInfo.getDpmBaseUrl() + fullPath);
    Invocation.Builder builder = resourceTarget.request(mediaType);
    builder.headers(request.getHeaders());
    Invocation invocation;
    if (request.getEntity() == null) {
      invocation = builder.build(method);
    } else {
      invocation = builder.build(method, Entity.entity(request.getEntity(), request.getMediaType()));
    }

    Response nextResponse = invocation.invoke();
    if (nextResponse.hasEntity()) {
      response.setEntityStream(nextResponse.readEntity(InputStream.class));
    }

    MultivaluedMap<String, String> headers = response.getHeaders();
    headers.clear();
    headers.putAll(nextResponse.getStringHeaders());
    response.setStatus(nextResponse.getStatus());
  }

}
