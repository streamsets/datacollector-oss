/*
 * Copyright 2018 StreamSets Inc.
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

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.PipelineInfoJson;
import com.streamsets.datacollector.restapi.bean.PipelineStateJson;
import com.streamsets.pipeline.api.ExecutionMode;
import org.apache.commons.lang3.StringUtils;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class EdgeUtil {
  public static final String EDGE_HTTP_URL = "edgeHttpUrl";

  public static void publishEdgePipeline(
      PipelineConfiguration pipelineConfiguration,
      String edgeHttpUrl

  ) throws PipelineException {
    String pipelineId = pipelineConfiguration.getPipelineId();

    PipelineConfigBean pipelineConfigBean =  PipelineBeanCreator.get()
        .create(pipelineConfiguration, new ArrayList<>(), null, null, null);
    if (!pipelineConfigBean.executionMode.equals(ExecutionMode.EDGE)) {
      throw new PipelineException(ContainerError.CONTAINER_01600, pipelineConfigBean.executionMode);
    }

    if (StringUtils.isEmpty(edgeHttpUrl)) {
      edgeHttpUrl = pipelineConfigBean.edgeHttpUrl;
    }

    Response response = null;
    try {
      UUID uuid;
      response = ClientBuilder.newClient()
          .target(edgeHttpUrl + "/rest/v1/pipeline/" + pipelineId)
          .request()
          .get();
      if (response.getStatus() == Response.Status.OK.getStatusCode()) {
        // Pipeline with same pipelineId already exist, update pipeline
        PipelineConfigurationJson pipelineConfigurationJson = response.readEntity(PipelineConfigurationJson.class);
        uuid = pipelineConfigurationJson.getUuid();
      } else {
        // Pipeline Doesn't exist, create new pipeline
        response.close();
        response = ClientBuilder.newClient()
            .target(edgeHttpUrl + "/rest/v1/pipeline/" + pipelineId)
            .queryParam("description", pipelineConfiguration.getDescription())
            .request()
            .put(Entity.json(BeanHelper.wrapPipelineConfiguration(pipelineConfiguration)));
        PipelineConfigurationJson pipelineConfigurationJson = response.readEntity(PipelineConfigurationJson.class);
        uuid = pipelineConfigurationJson.getUuid();
      }

      // update pipeline Configuration
      response.close();
      pipelineConfiguration.setUuid(uuid);
      response = ClientBuilder.newClient()
          .target(edgeHttpUrl + "/rest/v1/pipeline/" + pipelineId)
          .queryParam("pipelineTitle", pipelineConfiguration.getPipelineId())
          .queryParam("description", pipelineConfiguration.getDescription())
          .request()
          .post(Entity.json(BeanHelper.wrapPipelineConfiguration(pipelineConfiguration)));

      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new PipelineException(
            ContainerError.CONTAINER_01605,
            response.getStatus(),
            response.readEntity(String.class)
        );
      }

    } catch (ProcessingException ex) {
      if (ex.getCause() instanceof ConnectException) {
        throw new PipelineException(ContainerError.CONTAINER_01602, edgeHttpUrl, ex);
      }
      throw ex;
    }
    finally {
      if (response != null) {
        response.close();
      }
    }
  }

  public static PipelineStateJson getEdgePipelineState(
      PipelineConfiguration pipelineConfiguration
  ) throws PipelineException {
    String pipelineId = pipelineConfiguration.getPipelineId();
    PipelineConfigBean pipelineConfigBean =  PipelineBeanCreator.get()
        .create(pipelineConfiguration, new ArrayList<>(), null, null, null);
    if (!pipelineConfigBean.executionMode.equals(ExecutionMode.EDGE)) {
      throw new PipelineException(ContainerError.CONTAINER_01600, pipelineConfigBean.executionMode);
    }

    Response response = null;
    try {
      response = ClientBuilder.newClient()
          .target(pipelineConfigBean.edgeHttpUrl + "/rest/v1/pipeline/" + pipelineId + "/status")
          .request()
          .get();
      if (response.getStatus() == Response.Status.OK.getStatusCode()) {
        return response.readEntity(PipelineStateJson.class);
      } else {
        return null;
      }
    } catch (ProcessingException ex) {
      if (ex.getCause() instanceof ConnectException) {
        throw new PipelineException(ContainerError.CONTAINER_01602, pipelineConfigBean.edgeHttpUrl, ex);
      }
      throw ex;
    }
    finally {
      if (response != null) {
        response.close();
      }
    }
  }

  public static PipelineStateJson startEdgePipeline(
      PipelineConfiguration pipelineConfiguration,
      Map<String, Object> runtimeParameters
  ) throws PipelineException {
    String pipelineId = pipelineConfiguration.getPipelineId();
    PipelineConfigBean pipelineConfigBean =  PipelineBeanCreator.get()
        .create(pipelineConfiguration, new ArrayList<>(), runtimeParameters, null, null);
    if (!pipelineConfigBean.executionMode.equals(ExecutionMode.EDGE)) {
      throw new PipelineException(ContainerError.CONTAINER_01600, pipelineConfigBean.executionMode);
    }

    Response response = null;
    try {
      response = ClientBuilder.newClient()
          .target(pipelineConfigBean.edgeHttpUrl + "/rest/v1/pipeline/" + pipelineId + "/start")
          .request()
          .post(Entity.json(runtimeParameters));
      if (response.getStatus() == Response.Status.OK.getStatusCode()) {
        return response.readEntity(PipelineStateJson.class);
      } else {
        throw new PipelineException(
            ContainerError.CONTAINER_01603,
            response.getStatus(),
            response.readEntity(String.class)
        );
      }
    } catch (ProcessingException ex) {
      if (ex.getCause() instanceof ConnectException) {
        throw new PipelineException(ContainerError.CONTAINER_01602, pipelineConfigBean.edgeHttpUrl, ex);
      }
      throw ex;
    }
    finally {
      if (response != null) {
        response.close();
      }
    }
  }

  public static PipelineStateJson stopEdgePipeline(
      PipelineConfiguration pipelineConfiguration,
      Map<String, Object> runtimeParameters
  ) throws PipelineException {
    String pipelineId = pipelineConfiguration.getPipelineId();
    PipelineConfigBean pipelineConfigBean =  PipelineBeanCreator.get()
        .create(pipelineConfiguration, new ArrayList<>(), runtimeParameters, null, null);
    if (!pipelineConfigBean.executionMode.equals(ExecutionMode.EDGE)) {
      throw new PipelineException(ContainerError.CONTAINER_01600, pipelineConfigBean.executionMode);
    }

    Response response = null;
    try {
      response = ClientBuilder.newClient()
          .target(pipelineConfigBean.edgeHttpUrl + "/rest/v1/pipeline/" + pipelineId + "/stop")
          .request()
          .post(Entity.json(runtimeParameters));
      if (response.getStatus() == Response.Status.OK.getStatusCode()) {
        return response.readEntity(PipelineStateJson.class);
      } else {
        throw new PipelineException(
            ContainerError.CONTAINER_01603,
            response.getStatus(),
            response.readEntity(String.class)
        );
      }
    } catch (ProcessingException ex) {
      if (ex.getCause() instanceof ConnectException) {
        throw new PipelineException(ContainerError.CONTAINER_01602, pipelineConfigBean.edgeHttpUrl, ex);
      }
      throw ex;
    }
    finally {
      if (response != null) {
        response.close();
      }
    }
  }

  public static MetricRegistryJson getEdgePipelineMetrics(
      PipelineConfiguration pipelineConfiguration
  ) throws PipelineException {
    String pipelineId = pipelineConfiguration.getPipelineId();
    PipelineConfigBean pipelineConfigBean =  PipelineBeanCreator.get()
        .create(pipelineConfiguration, new ArrayList<>(), null, null, null);
    if (!pipelineConfigBean.executionMode.equals(ExecutionMode.EDGE)) {
      throw new PipelineException(ContainerError.CONTAINER_01600, pipelineConfigBean.executionMode);
    }

    Response response = null;
    try {
      response = ClientBuilder.newClient()
          .target(pipelineConfigBean.edgeHttpUrl + "/rest/v1/pipeline/" + pipelineId + "/metrics")
          .request()
          .get();
      if (response.getStatus() == Response.Status.OK.getStatusCode()) {
        return response.readEntity(MetricRegistryJson.class);
      } else {
        return null;
      }
    } catch (ProcessingException ex) {
      if (ex.getCause() instanceof ConnectException) {
        throw new PipelineException(ContainerError.CONTAINER_01602, pipelineConfigBean.edgeHttpUrl, ex);
      }
      throw ex;
    }
    finally {
      if (response != null) {
        response.close();
      }
    }
  }

  public static void resetOffset(PipelineConfiguration pipelineConfiguration) throws PipelineException {
    String pipelineId = pipelineConfiguration.getPipelineId();
    PipelineConfigBean pipelineConfigBean =  PipelineBeanCreator.get()
        .create(pipelineConfiguration, new ArrayList<>(), null, null, null);
    if (!pipelineConfigBean.executionMode.equals(ExecutionMode.EDGE)) {
      throw new PipelineException(ContainerError.CONTAINER_01600, pipelineConfigBean.executionMode);
    }
    Response response = null;
    try {
      response = ClientBuilder.newClient()
          .target(pipelineConfigBean.edgeHttpUrl + "/rest/v1/pipeline/" + pipelineId + "/resetOffset")
          .request()
          .post(null);
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new PipelineException(
            ContainerError.CONTAINER_01604,
            response.getStatus(),
            response.readEntity(String.class)
        );
      }
    } catch (ProcessingException ex) {
      if (ex.getCause() instanceof ConnectException) {
        throw new PipelineException(ContainerError.CONTAINER_01602, pipelineConfigBean.edgeHttpUrl, ex);
      }
      throw ex;
    }
    finally {
      if (response != null) {
        response.close();
      }
    }
  }

  public static List<PipelineInfoJson> getEdgePipelines(String edgeHttpUrl) throws PipelineException {
    Response response = null;
    try {
      response = ClientBuilder.newClient()
          .target(edgeHttpUrl + "/rest/v1/pipelines")
          .request()
          .get();
      if (response.getStatus() == Response.Status.OK.getStatusCode()) {
        return Arrays.asList(response.readEntity(PipelineInfoJson[].class));
      } else {
        return Collections.emptyList();
      }
    } catch (ProcessingException ex) {
      if (ex.getCause() instanceof ConnectException) {
        throw new PipelineException(ContainerError.CONTAINER_01602, edgeHttpUrl, ex);
      }
      throw ex;
    }
    finally {
      if (response != null) {
        response.close();
      }
    }
  }

  public static PipelineConfigurationJson getEdgePipeline(String edgeHttpUrl, String pipelineId) throws PipelineException {
    Response response = null;
    try {
      response = ClientBuilder.newClient()
          .target(edgeHttpUrl + "/rest/v1/pipeline/" + pipelineId)
          .request()
          .get();
      if (response.getStatus() == Response.Status.OK.getStatusCode()) {
        return response.readEntity(PipelineConfigurationJson.class);
      } else {
        return null;
      }
    } catch (ProcessingException ex) {
      if (ex.getCause() instanceof ConnectException) {
        throw new PipelineException(ContainerError.CONTAINER_01602, edgeHttpUrl, ex);
      }
      throw ex;
    }
    finally {
      if (response != null) {
        response.close();
      }
    }
  }

  public static Response proxyRequestGET(String resourceUrl, String resource, Map<String, Object> params) {
    return proxyRequest("GET", resourceUrl, resource, params, null);
  }

  public static Response proxyRequestPOST(
      String resourceUrl,
      String resource,
      Map<String, Object> params,
      Object payload
  ) {
    return proxyRequest("POST", resourceUrl, resource, params, payload);
  }

  public static Response proxyRequestDELETE(String resourceUrl, String resource, Map<String, Object> params) {
    return proxyRequest("DELETE", resourceUrl, resource, params, null);
  }

  private static Response proxyRequest(
      String method,
      String resourceUrl,
      String resource,
      Map<String, Object> params,
      Object payload
  ) {
    WebTarget target = ClientBuilder.newClient().target(resourceUrl).path(resource);
    for (Map.Entry<String, Object> entry : params.entrySet()) {
      target = target.queryParam(entry.getKey(), entry.getValue());
    }
    if ("GET".equals(method)) {
      return target.request().get();
    } if ("POST".equals(method)) {
      if (payload != null) {
        return target.request().post(Entity.entity(payload, MediaType.APPLICATION_JSON));
      } else {
        return target.request().method("POST");
      }
    } if ("DELETE".equals(method)) {
      return target.request().delete();
    } else {
      throw new RuntimeException("Unsupported HTTP method for proxying: " + method);
    }
  }

}
