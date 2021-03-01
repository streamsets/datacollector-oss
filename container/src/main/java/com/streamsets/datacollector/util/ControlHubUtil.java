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
package com.streamsets.datacollector.util;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.event.client.impl.MovedDpmJerseyClientFilter;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.execution.CommitPipelineJson;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.lib.security.http.DpmClientInfo;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.lib.security.http.SSOPrincipal;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Map;

public class ControlHubUtil {

  public static PipelineConfigurationJson publishPipeline(
      HttpServletRequest request,
      DpmClientInfo dpmClientInfo,
      CommitPipelineJson commitPipelineModel
  ) throws IOException, PipelineException {
    SSOPrincipal ssoPrincipal = (SSOPrincipal)request.getUserPrincipal();
    String userAuthToken = ssoPrincipal.getTokenStr();

    ClientConfig clientConfig = new ClientConfig();
    clientConfig.register(new MovedDpmJerseyClientFilter(dpmClientInfo));
    Client client = ClientBuilder.newClient(clientConfig);

    try (Response response = client
        .target(dpmClientInfo.getDpmBaseUrl() + "pipelinestore/rest/v1/pipelines")
        .request()
        .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
        .header(SSOConstants.X_REST_CALL, SSOConstants.SDC_COMPONENT_NAME)
        .put(Entity.json(commitPipelineModel))) {
      if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
        Map<String, Object> responseData = response.readEntity(Map.class);
        return ObjectMapperFactory.get().readValue(
            (String)responseData.get("pipelineDefinition"),
            PipelineConfigurationJson.class
        );
      } else {
        throw new PipelineException(
            ContainerError.CONTAINER_01700,
            response.getStatus(),
            response.readEntity(String.class)
        );
      }
    }
  }

  public static Response getPipelines(
      HttpServletRequest request,
      DpmClientInfo dpmClientInfo,
      int offset,
      int len,
      String executionModes
  ) {
    return getProxyMethod(
        request,
        dpmClientInfo,
        "pipelinestore/rest/v1/pipelines",
        ImmutableMap.of(
            "offset", offset,
            "len", len,
            "executionModes", executionModes
        )
    );
  }

  public static Response getPipeline(
      HttpServletRequest request,
      DpmClientInfo dpmClientInfo,
      String pipelineCommitId
  ) {
    return getProxyMethod(
        request,
        dpmClientInfo,
        "pipelinestore/rest/v1/pipelineCommit/" + pipelineCommitId,
        null
    );
  }

  public static Response getPipelineCommitHistory(
      HttpServletRequest request,
      DpmClientInfo dpmClientInfo,
      String pipelineId,
      int offset,
      int len,
      String order
  ) {
    return getProxyMethod(
        request,
        dpmClientInfo,
        "pipelinestore/rest/v1/pipeline/" + pipelineId + "/log",
        ImmutableMap.of(
            "offset", offset,
            "len", len,
            "order", order
        )
    );
  }

  public static Response getRemoteRoles(
      HttpServletRequest request,
      DpmClientInfo dpmClientInfo
  ) {
    return getProxyMethod(
        request,
        dpmClientInfo,
         "security/rest/v1/currentUser",
        null
    );
  }

  public static Response getControlHubUsers(
      HttpServletRequest request,
      DpmClientInfo dpmClientInfo,
      int offset,
      int len
  ) {
    String orgId = getOrganizationId(request);
    return getProxyMethod(
        request,
        dpmClientInfo,
        "security/rest/v1/organization/" + orgId + "/users",
        ImmutableMap.of(
            "offset", offset,
            "len", len
        )
    );
  }

  public static Response getControlHubGroups(
      HttpServletRequest request,
      DpmClientInfo dpmClientInfo,
      int offset,
      int len
  ) {
    String orgId = getOrganizationId(request);
    return getProxyMethod(
        request,
        dpmClientInfo,
        "security/rest/v1/organization/" + orgId + "/groups",
        ImmutableMap.of(
            "offset", offset,
            "len", len
        )
    );
  }

  private static Response getProxyMethod(
      HttpServletRequest request,
      DpmClientInfo dpmClientInfo,
      String resourceUrl,
      Map<String, Object> queryParams
  ) {
    ClientConfig clientConfig = new ClientConfig();
    clientConfig.register(new MovedDpmJerseyClientFilter(dpmClientInfo));
    Client client = ClientBuilder.newClient(clientConfig);

    SSOPrincipal ssoPrincipal = (SSOPrincipal)request.getUserPrincipal();
    String userAuthToken = ssoPrincipal.getTokenStr();
    WebTarget webTarget = client.target(dpmClientInfo.getDpmBaseUrl() + resourceUrl);

    if (queryParams != null) {
      queryParams.forEach(webTarget::queryParam);
    }

    return webTarget
        .request()
        .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
        .header(SSOConstants.X_REST_CALL, SSOConstants.SDC_COMPONENT_NAME)
        .get();
  }

  private static String getOrganizationId(HttpServletRequest request) {
    SSOPrincipal ssoPrincipal = (SSOPrincipal)request.getUserPrincipal();
    return ssoPrincipal.getOrganizationId();
  }
}
