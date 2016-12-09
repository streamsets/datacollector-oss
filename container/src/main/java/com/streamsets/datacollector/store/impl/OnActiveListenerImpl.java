/*
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.store.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;
import com.streamsets.datacollector.execution.store.CuratorFrameworkConnector;
import com.streamsets.datacollector.execution.store.FatalZKStoreException;
import com.streamsets.datacollector.execution.store.OnActiveListener;
import com.streamsets.datacollector.execution.store.ZKStoreException;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineStateJson;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.ContainerError;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.grizzly.connector.GrizzlyConnectorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;

public class OnActiveListenerImpl implements OnActiveListener {
  private static final Logger LOG = LoggerFactory.getLogger(OnActiveListener.class);

  public void onActive(CuratorFrameworkConnector curator) {
    ClientConfig clientConfig = new ClientConfig()
        .connectorProvider(new GrizzlyConnectorProvider());

    Client client = ClientBuilder.newBuilder().withConfig(clientConfig).build();

    List<String> pipelines;
    try {
      pipelines = curator.getPipelines();
    } catch (ZKStoreException e) {
      throw new FatalZKStoreException(e);
    }

    for (String pipeline : pipelines) {
      try {
        String pipelineJson = curator.getPipelineState(pipeline);
        if (pipelineJson == null) {
          return;
        }
        PipelineStateJson pipelineStateJson = ObjectMapperFactory.get().readValue(pipelineJson, PipelineStateJson.class);
        if (pipelineStateJson.getPipelineState().getStatus().isActive()) {
          // force to inactive state
          PipelineState oldPipelineState = pipelineStateJson.getPipelineState();
          PipelineState pipelineState = new PipelineStateImpl(
              oldPipelineState.getUser(),
              oldPipelineState.getName(),
              oldPipelineState.getRev(),
              PipelineStatus.STOPPED,
              oldPipelineState.getMessage(),
              oldPipelineState.getTimeStamp(),
              oldPipelineState.getAttributes(),
              oldPipelineState.getExecutionMode(),
              oldPipelineState.getMetrics(),
              oldPipelineState.getRetryAttempt(),
              oldPipelineState.getNextRetryTimeStamp()
          );
          curator.updatePipelineState(pipeline, serializePipelineState(pipelineState));
          LOG.debug("Forced pipeline into terminal state.");
        }
      } catch (ZKStoreException | IOException | PipelineStoreException e) {
        throw new FatalZKStoreException(e);
      }

      LOG.debug("Attempting to start: " + pipeline);
      WebTarget startPipeline = client.target("http://localhost:18630/rest/v1/pipeline/" + pipeline + "/start");
      Response r = startPipeline.request().header("X-Requested-By", "DataCollector").method("POST");
      LOG.debug("Response from Starting Request: {}", r.toString());
    }
  }

  private String serializePipelineState(PipelineState pipelineState) throws PipelineStoreException {
    PipelineStateJson pipelineStateJson = BeanHelper.wrapPipelineState(pipelineState);
    try {
      return ObjectMapperFactory.get().writeValueAsString(pipelineStateJson);
    } catch (JsonProcessingException e) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0210, e.toString(), e);
    }
  }
}
