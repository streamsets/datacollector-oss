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
import com.streamsets.datacollector.restapi.bean.MetricRegistryJson;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.PipelineStateJson;
import com.streamsets.pipeline.api.ExecutionMode;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

public class EdgeUtil {

  public static void publishEdgePipeline(PipelineConfiguration pipelineConfiguration) throws PipelineException {
    String pipelineId = pipelineConfiguration.getPipelineId();
    PipelineConfigBean pipelineConfigBean =  PipelineBeanCreator.get()
        .create(pipelineConfiguration, new ArrayList<>(), null);
    if (!pipelineConfigBean.executionMode.equals(ExecutionMode.EDGE)) {
      throw new PipelineException(ContainerError.CONTAINER_01600, pipelineConfigBean.executionMode);
    }

    Response response = null;
    try {
      UUID uuid;
      response = ClientBuilder.newClient()
          .target(pipelineConfigBean.edgeHttpUrl + "/rest/v1/pipeline/" + pipelineId)
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
            .target(pipelineConfigBean.edgeHttpUrl + "/rest/v1/pipeline/" + pipelineId)
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
          .target(pipelineConfigBean.edgeHttpUrl + "/rest/v1/pipeline/" + pipelineId)
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

  public static PipelineStateJson getEdgePipelineState(
      PipelineConfiguration pipelineConfiguration
  ) throws PipelineException {
    String pipelineId = pipelineConfiguration.getPipelineId();
    PipelineConfigBean pipelineConfigBean =  PipelineBeanCreator.get()
        .create(pipelineConfiguration, new ArrayList<>(), null);
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
        .create(pipelineConfiguration, new ArrayList<>(), runtimeParameters);
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
        .create(pipelineConfiguration, new ArrayList<>(), runtimeParameters);
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
        .create(pipelineConfiguration, new ArrayList<>(), null);
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
        .create(pipelineConfiguration, new ArrayList<>(), null);
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

}
