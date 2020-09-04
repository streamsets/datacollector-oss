/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.config.HttpMethod;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.gateway.GatewayInfo;
import com.streamsets.pipeline.api.impl.Utils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.InputStream;

public abstract class GatewayBaseResource {

  private final static String GATEWAY_SECRET = "GATEWAY_SECRET";
  protected RuntimeInfo runtimeInfo;
  protected Manager manager;
  protected PipelineStoreTask store;

  @Path("/{subResources: .*}")
  @GET
  public Response handleGetRequests(
      @PathParam("subResources") String subResources,
      @Context HttpServletRequest request,
      @Context UriInfo uriInfo,
      @Context HttpHeaders httpheaders
  ) throws PipelineException {
    return proxyRequest(subResources, request, uriInfo, HttpMethod.GET, httpheaders, null);
  }

  @Path("/{subResources: .*}")
  @POST
  public Response handlePostRequests(
      @PathParam("subResources") String subResources,
      @Context HttpServletRequest request,
      @Context UriInfo uriInfo,
      @Context HttpHeaders httpheaders,
      InputStream inputStream
  ) throws PipelineException {
    return proxyRequest(subResources, request, uriInfo, HttpMethod.POST, httpheaders, inputStream);
  }

  @Path("/{subResources: .*}")
  @PUT
  public Response handlePutRequests(
      @PathParam("subResources") String subResources,
      @Context HttpServletRequest request,
      @Context UriInfo uriInfo,
      @Context HttpHeaders httpheaders,
      InputStream inputStream
  ) throws PipelineException {
    return proxyRequest(subResources, request, uriInfo, HttpMethod.PUT, httpheaders, inputStream);
  }

  @Path("/{subResources: .*}")
  @DELETE
  public Response handleDeleteRequests(
      @PathParam("subResources") String subResources,
      @Context HttpServletRequest request,
      @Context UriInfo uriInfo,
      @Context HttpHeaders httpheaders
  ) throws PipelineException {
    return proxyRequest(subResources, request, uriInfo, HttpMethod.DELETE, httpheaders,null);
  }

  @Path("/{subResources: .*}")
  @HEAD
  public Response handleHeadRequests(
      @PathParam("subResources") String subResources,
      @Context HttpServletRequest request,
      @Context UriInfo uriInfo,
      @Context HttpHeaders httpheaders
  ) throws PipelineException {
    return proxyRequest(subResources, request, uriInfo, HttpMethod.HEAD, httpheaders,null);
  }

  private Response proxyRequest(
      String subResources,
      HttpServletRequest httpServletRequest,
      UriInfo uriInfo,
      HttpMethod httpMethod,
      HttpHeaders httpheaders,
      InputStream inputStream
  ) throws PipelineException {
    String serviceName = subResources.split("/")[0];

    GatewayInfo gatewayInfo = runtimeInfo.getApiGateway(serviceName);
    Utils.checkState(
        gatewayInfo != null,
        Utils.format("No entry found in the gateway registry for the service name: {}", serviceName)
    );

    validateRequest(gatewayInfo);

    String resourceUrl = gatewayInfo.getServiceUrl() +
        getBaseUrlPath() +
        uriInfo.getPath();

    if (httpServletRequest.getQueryString() != null) {
      resourceUrl += "?" + httpServletRequest.getQueryString();
    }

    WebTarget target = ClientBuilder.newClient().target(resourceUrl);

    Invocation.Builder request = target.request();

    // update request headers
    httpheaders.getRequestHeaders().forEach((k, l) -> l.forEach(v -> request.header(k, v)));

    // To avoid users calling REST Service URL directly instead of going through gateway URL.
    // We use GATEWAY_SECRET header, and REST Service will process request only if secret matches
    request.header(GATEWAY_SECRET, gatewayInfo.getSecret());

    String contentType = httpheaders.getRequestHeaders().getFirst(HttpHeaders.CONTENT_TYPE);
    if (contentType == null) {
      contentType = MediaType.TEXT_PLAIN;
    }

    // update request cookies
    httpheaders.getCookies().forEach((k, v) -> request.cookie(k, v.getValue()));

    switch (httpMethod) {
      case GET:
        return request.get();
      case POST:
        if (inputStream != null) {
          return request.post(Entity.entity(inputStream, contentType));
        } else {
          return request.method(httpMethod.getLabel());
        }
      case PUT:
        if (inputStream != null) {
          return request.put(Entity.entity(inputStream, contentType));
        } else {
          return request.method(httpMethod.getLabel());
        }
      case DELETE:
        return request.delete();
      case HEAD:
        return request.head();
      default:
        throw new RuntimeException("Unsupported HTTP method: " + httpMethod);
    }
  }

  protected void validateRequest(GatewayInfo gatewayInfo) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(gatewayInfo.getPipelineId());
    Utils.checkState(pipelineInfo != null, "Invalid Pipeline Id");

    PipelineState pipelineState = manager.getPipelineState(gatewayInfo.getPipelineId(), "0");
    Utils.checkState(
        pipelineState != null && pipelineState.getStatus().isActive(),
        "Pipeline is not active"
    );
  }

  protected abstract String getBaseUrlPath();

}
