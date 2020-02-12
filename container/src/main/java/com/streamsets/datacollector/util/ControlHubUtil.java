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
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.execution.CommitPipelineJson;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.lib.security.http.SSOPrincipal;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Map;

public class ControlHubUtil {

  public static PipelineConfigurationJson publishPipeline(
      HttpServletRequest request,
      String controlHubBaseUrl,
      CommitPipelineJson commitPipelineModel
  ) throws IOException, PipelineException {
    SSOPrincipal ssoPrincipal = (SSOPrincipal)request.getUserPrincipal();
    String userAuthToken = ssoPrincipal.getTokenStr();
    try (Response response = ClientBuilder.newClient()
        .target(controlHubBaseUrl + "pipelinestore/rest/v1/pipelines")
        .register(new CsrfProtectionFilter())
        .request()
        .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
        .header(SSOConstants.X_REST_CALL, true)
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
      String controlHubBaseUrl,
      int offset,
      int len,
      String executionModes
  ) {
    return getProxyMethod(
        request,
        controlHubBaseUrl + "pipelinestore/rest/v1/pipelines",
        ImmutableMap.of(
            "offset", offset,
            "len", len,
            "executionModes", executionModes
        )
    );
  }

  public static Response getPipeline(
      HttpServletRequest request,
      String controlHubBaseUrl,
      String pipelineCommitId
  ) {
    return getProxyMethod(
        request,
        controlHubBaseUrl + "pipelinestore/rest/v1/pipelineCommit/" + pipelineCommitId,
        null
    );
  }

  public static Response getPipelineCommitHistory(
      HttpServletRequest request,
      String controlHubBaseUrl,
      String pipelineId,
      int offset,
      int len,
      String order
  ) {
    return getProxyMethod(
        request,
        controlHubBaseUrl + "pipelinestore/rest/v1/pipeline/" + pipelineId + "/log",
        ImmutableMap.of(
            "offset", offset,
            "len", len,
            "order", order
        )
    );
  }

  public static Response getRemoteRoles(HttpServletRequest request, String controlHubBaseUrl) {
    return getProxyMethod(
        request,
        controlHubBaseUrl + "security/rest/v1/currentUser",
        null
    );
  }

  public static Response getControlHubUsers(
      HttpServletRequest request,
      String controlHubBaseUrl,
      int offset,
      int len
  ) {
    String orgId = getOrganizationId(request);
    return getProxyMethod(
        request,
        controlHubBaseUrl + "security/rest/v1/organization/" + orgId + "/users",
        ImmutableMap.of(
            "offset", offset,
            "len", len
        )
    );
  }

  public static Response getControlHubGroups(
      HttpServletRequest request,
      String controlHubBaseUrl,
      int offset,
      int len
  ) {
    String orgId = getOrganizationId(request);
    return getProxyMethod(
        request,
        controlHubBaseUrl + "security/rest/v1/organization/" + orgId + "/groups",
        ImmutableMap.of(
            "offset", offset,
            "len", len
        )
    );
  }

  private static Response getProxyMethod(
      HttpServletRequest request,
      String resourceUrl,
      Map<String, Object> queryParams
  ) {
    SSOPrincipal ssoPrincipal = (SSOPrincipal)request.getUserPrincipal();
    String userAuthToken = ssoPrincipal.getTokenStr();
    WebTarget webTarget = ClientBuilder.newClient()
        .target(resourceUrl)
        .register(new CsrfProtectionFilter());

    if (queryParams != null) {
      queryParams.forEach(webTarget::queryParam);
    }

    return webTarget
        .request()
        .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
        .header(SSOConstants.X_REST_CALL, true)
        .get();
  }

  private static String getOrganizationId(HttpServletRequest request) {
    SSOPrincipal ssoPrincipal = (SSOPrincipal)request.getUserPrincipal();
    return ssoPrincipal.getOrganizationId();
  }
}
