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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.execution.store;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollector;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineStateJson;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.impl.OnActiveListenerImpl;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LogUtil;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.ext.DataCollectorServices;
import com.streamsets.pipeline.api.impl.Utils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.streamsets.datacollector.execution.store.FilePipelineStateStore.STATE;

public class ZooKeeperPipelineStateStore implements PipelineStateStore {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperPipelineStateStore.class);
  private static final String ZK_URI_KEY = "zookeeper.uri";
  private static final String ZK_RETRIES_KEY = "zookeeper.retries";
  private static final String ZK_ZNODE_KEY = "zookeeper.znode";

  private final CuratorFrameworkConnector curator;

  @Inject
  public ZooKeeperPipelineStateStore(RuntimeInfo runtimeInfo) {
    CuratorFrameworkConnector curator = DataCollectorServices.instance().get(CuratorFrameworkConnector.SERVICE_NAME);

    if (curator == null) {
      File sdcPropertiesFile = new File(runtimeInfo.getConfigDir(), "sdc.properties");
      Properties sdcProperties = new Properties();
      try {
        sdcProperties.load(new FileInputStream(sdcPropertiesFile));
      } catch (IOException e) {
        throw new FatalZKStoreException("Could not load sdc.properties");
      }
      final String zkConnectionString = sdcProperties.getProperty(ZK_URI_KEY);
      final String znode = sdcProperties.getProperty(ZK_ZNODE_KEY, "/datacollector");
      final int retries = Integer.parseInt(sdcProperties.getProperty(ZK_RETRIES_KEY, "0"));
      curator = new CuratorFrameworkConnector(zkConnectionString, znode, retries, new OnActiveListenerImpl());

      DataCollectorServices.instance().put(CuratorFrameworkConnector.SERVICE_NAME, curator);
    }

    this.curator = curator;
  }

  @Override
  public void init() {
  }

  @Override
  public void destroy() {
  }

  @Override
  public PipelineState edited(
      String user, String name, String rev, ExecutionMode executionMode, boolean isRemote
  ) throws PipelineStoreException {
    PipelineState pipelineState = getState(name, rev);

    if (pipelineState != null) {
      Utils.checkState(
          !pipelineState.getStatus().isActive(),
          Utils.format("Cannot edit pipeline in state: '{}'", pipelineState.getStatus())
      );
    }

    // first time when pipeline is created
    Map<String, Object> attributes = null;
    if (pipelineState == null) {
      attributes = new HashMap<>();
      attributes.put(RemoteDataCollector.IS_REMOTE_PIPELINE, isRemote);
    }
    if (pipelineState == null || pipelineState.getStatus() != PipelineStatus.EDITED || executionMode != pipelineState
        .getExecutionMode()) {
      return saveState(user,
          name,
          rev,
          PipelineStatus.EDITED,
          "Pipeline edited",
          attributes,
          executionMode,
          null,
          0,
          0
      );
    } else {
      return null;
    }
  }

  private String getNameAndRevString(String name, String rev) {
    return name + "::" + rev;
  }

  @Override
  public void delete(String name, String rev) {
    try {
      curator.deletePipelineState(name);
    } catch (ZKStoreException e) {
      throw new FatalZKStoreException("Could not delete pipeline state for " + name, e);
    }
  }

  @Override
  public PipelineState saveState(
      String user,
      String name,
      String rev,
      PipelineStatus status,
      String message,
      Map<String, Object> attributes,
      ExecutionMode executionMode,
      String metrics,
      int retryAttempt,
      long nextRetryTimeStamp
  ) throws PipelineStoreException {
    LOG.debug("Changing state of pipeline '{}','{}','{}' to '{}' in execution mode: '{}';" + "status msg is '{}'",
        name,
        rev,
        user,
        status,
        executionMode,
        message
    );

    try {
      String pipelineStateJson = curator.getPipelineState(name);
      if (pipelineStateJson != null && attributes == null) {
        attributes = getState(name, rev).getAttributes();
      }

      PipelineState pipelineState = new PipelineStateImpl(user,
          name,
          rev,
          status,
          message,
          System.currentTimeMillis(),
          attributes,
          executionMode,
          metrics,
          retryAttempt,
          nextRetryTimeStamp
      );

      final String newPipelineState = serializePipelineState(pipelineState);
      curator.updatePipelineState(name, newPipelineState);
      curator.updatePipelineStateHistory(name, newPipelineState);

      return pipelineState;

    } catch (NotPrimaryException e) {
      throw new FatalZKStoreException("This DataCollector is not the primary. Cannot save pipeline state.", e);
    } catch (ZKStoreException e) {
      throw new FatalZKStoreException(e);
    }
  }

  @Nullable
  private PipelineState loadState(String nameAndRev) throws PipelineStoreException {
    try {
      String pipelineJson = curator.getPipelineState(nameAndRev);
      if (pipelineJson == null) {
        return null;
      }
      PipelineStateJson pipelineStateJson = ObjectMapperFactory.get().readValue(pipelineJson, PipelineStateJson.class);
      return pipelineStateJson.getPipelineState();
    } catch (ZKStoreException | IOException e) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0001, e.toString(), e);
    }
  }

  @Override
  @Nullable
  public PipelineState getState(String name, String rev) throws PipelineStoreException {
    return loadState(name);
  }

  @Override
  public List<PipelineState> getHistory(String pipelineName, String rev, boolean fromBeginning) throws
      PipelineStoreException {
    try {
      String stateHistoryJson = curator.getPipelineStateHistory(pipelineName);

      ObjectMapper objectMapper = ObjectMapperFactory.get();
      JsonParser jsonParser = objectMapper.getFactory().createParser(stateHistoryJson);
      MappingIterator<PipelineStateJson> pipelineStateMappingIterator = objectMapper.readValues(jsonParser,
          PipelineStateJson.class
      );
      List<PipelineStateJson> pipelineStateJsons = pipelineStateMappingIterator.readAll();
      Collections.reverse(pipelineStateJsons);
      if (fromBeginning) {
        return BeanHelper.unwrapPipelineStatesNewAPI(pipelineStateJsons);
      } else {
        int toIndex = pipelineStateJsons.size() > 100 ? 100 : pipelineStateJsons.size();
        return BeanHelper.unwrapPipelineStatesNewAPI(pipelineStateJsons.subList(0, toIndex));
      }
    } catch (ZKStoreException e) {
      throw new FatalZKStoreException(e);
    } catch (IOException e) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0115, pipelineName, rev, e.toString(), e);
    }
  }

  @Override
  public void deleteHistory(String pipelineName, String rev) {
    try {
      curator.deletePipelineStateHistory(pipelineName);
    } catch (ZKStoreException e) {
      throw new FatalZKStoreException(e);
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
