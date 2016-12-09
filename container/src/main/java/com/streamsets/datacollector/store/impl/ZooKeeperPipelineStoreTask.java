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
package com.streamsets.datacollector.store.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollector;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;
import com.streamsets.datacollector.execution.store.CuratorFrameworkConnector;
import com.streamsets.datacollector.execution.store.FatalZKStoreException;
import com.streamsets.datacollector.execution.store.NotPrimaryException;
import com.streamsets.datacollector.execution.store.ZKStoreException;
import com.streamsets.datacollector.io.DataStore;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.PipelineInfoJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineRevInfo;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.LogUtil;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.ext.DataCollectorServices;
import com.streamsets.pipeline.api.impl.PipelineUtils;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ZooKeeperPipelineStoreTask extends AbstractTask implements PipelineStoreTask {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperPipelineStoreTask.class);
  private static final String REV = "0";
  private static final String ZK_URI_KEY = "zookeeper.uri";
  private static final String ZK_RETRIES_KEY = "zookeeper.retries";
  private static final String ZK_ZNODE_KEY = "zookeeper.znode";

  private final CuratorFrameworkConnector curator;
  private final StageLibraryTask stageLibrary;
  private final ObjectMapper json;
  private final PipelineStateStore pipelineStateStore;
  private final ConcurrentMap<String, RuleDefinitions> pipelineToRuleDefinitionMap;
  private StateEventListener stateEventListener;

  @Inject
  public ZooKeeperPipelineStoreTask(
      RuntimeInfo runtimeInfo,
      StageLibraryTask stageLibrary,
      PipelineStateStore pipelineStateStore
  ) {
    super("ZooKeeperPipelineStore");
    this.stageLibrary = stageLibrary;

    json = ObjectMapperFactory.get();
    pipelineToRuleDefinitionMap = new ConcurrentHashMap<>();
    this.pipelineStateStore = pipelineStateStore;

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
  public void registerStateListener(StateEventListener stateListener) {
    stateEventListener = stateListener;
  }

  @Override
  public void initTask() {
    if (pipelineStateStore != null) {
      pipelineStateStore.init();
    }
  }

  @Override
  public void stopTask() {
    if (pipelineStateStore != null) {
      pipelineStateStore.destroy();
    }
  }

  @Override
  public boolean hasPipeline(String name) {
    try {
      return curator.getPipeline(name) != null;
    } catch (ZKStoreException e) {
      throw new FatalZKStoreException(e);
    }
  }

  @Override
  public PipelineConfiguration create(String user, String name, String description, boolean isRemote) throws
      PipelineStoreException {
    Date date = new Date();
    UUID uuid = UUID.randomUUID();
    PipelineInfo info = new PipelineInfo(name, description, date, date, user, user, REV, uuid, false, null);
    PipelineConfiguration pipeline = new PipelineConfiguration(SCHEMA_VERSION,
        PipelineConfigBean.VERSION,
        uuid,
        description,
        stageLibrary.getPipeline().getPipelineDefaultConfigs(),
        Collections.<String, Object>emptyMap(),
        Collections.<StageConfiguration>emptyList(),
        null,
        null
    );

    try (ByteArrayOutputStream infoFile = new ByteArrayOutputStream(); ByteArrayOutputStream pipelineFile = new
        ByteArrayOutputStream()) {
      json.writeValue(infoFile, BeanHelper.wrapPipelineInfo(info));
      json.writeValue(pipelineFile, BeanHelper.wrapPipelineConfiguration(pipeline));

      curator.createPipeline(name);
      curator.updatePipeline(name, pipelineFile.toString(Charsets.UTF_8.name()));
      curator.updateInfo(name, infoFile.toString(Charsets.UTF_8.name()));
    } catch (Exception ex) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0202, name, ex.toString(), ex);
    }
    pipeline.setPipelineInfo(info);
    pipelineStateStore.edited(user, name, REV, ExecutionMode.STANDALONE, isRemote);
    return pipeline;
  }

  @Override
  public void delete(String name) throws PipelineStoreException {
    if (!hasPipeline(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
    }
    if (pipelineStateStore == null) {
      return;
    }

    // For now, passing rev 0 - make delete take tag/rev as a parameter
    PipelineState currentState = pipelineStateStore.getState(name, REV);
    PipelineStatus pipelineStatus = currentState.getStatus();
    if (pipelineStatus.isActive()) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0208, pipelineStatus);
    }

    PipelineState latestState = new PipelineStateImpl(currentState.getUser(),
        currentState.getName(),
        currentState.getRev(),
        PipelineStatus.DELETED,
        "Pipeline is deleted",
        System.currentTimeMillis(),
        currentState.getAttributes(),
        currentState.getExecutionMode(),
        currentState.getMetrics(),
        currentState.getRetryAttempt(),
        currentState.getNextRetryTimeStamp()
    );
    try {
      if (stateEventListener != null) {
        stateEventListener.onStateChange(currentState, latestState, "", null);
      }
    } catch (Exception e) {
      LOG.warn("Cannot set delete event for pipeline");
    }
  }

  @Override
  public List<PipelineInfo> getPipelines() throws PipelineStoreException {
    List<PipelineInfo> pipelineInfoList = new ArrayList<>();
    try {
      for (String name : curator.getPipelines()) {
        PipelineInfoJson pipelineInfoJsonBean;
        LOG.info("PipelineName: {}", name);
        try {
          String infoJson = curator.getInfo(name);
          pipelineInfoJsonBean = json.readValue(infoJson, PipelineInfoJson.class);
        } catch (IOException e) {
          throw new PipelineStoreException(ContainerError.CONTAINER_0206, name, e);
        }
        pipelineInfoList.add(pipelineInfoJsonBean.getPipelineInfo());
      }

    } catch (ZKStoreException e) {
      throw new FatalZKStoreException(e);
    }

    return Collections.unmodifiableList(pipelineInfoList);
  }

  @Override
  public PipelineInfo getInfo(String name) throws PipelineStoreException {
    return getInfo(name, false);
  }

  private PipelineInfo getInfo(String name, boolean checkExistence) throws PipelineStoreException {
    if (checkExistence && !hasPipeline(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
    }
    try {
      String infoJson = curator.getInfo(name);
      PipelineInfoJson pipelineInfoJsonBean = json.readValue(infoJson, PipelineInfoJson.class);
      return pipelineInfoJsonBean.getPipelineInfo();
    } catch (Exception ex) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0206, name, ex);
    }
  }

  @Override
  public PipelineConfiguration save(
      String user, String name, String tag, String tagDescription, PipelineConfiguration pipeline
  ) throws PipelineStoreException {
    if (!hasPipeline(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
    }
    PipelineInfo savedInfo = getInfo(name);
    if (!savedInfo.getUuid().equals(pipeline.getUuid())) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0205, name);
    }
    if (pipelineStateStore != null) {
      PipelineStatus pipelineStatus = pipelineStateStore.getState(name, tag).getStatus();
      if (pipelineStatus.isActive()) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0208, pipelineStatus);
      }
    }
    UUID uuid = UUID.randomUUID();
    PipelineInfo info = new PipelineInfo(getInfo(name),
        pipeline.getDescription(),
        new Date(),
        user,
        REV,
        uuid,
        pipeline.isValid(),
        pipeline.getMetadata()
    );
    try (ByteArrayOutputStream infoOut = new ByteArrayOutputStream(); ByteArrayOutputStream pipelineOut = new
        ByteArrayOutputStream()) {
      pipeline.setUuid(uuid);
      json.writeValue(infoOut, BeanHelper.wrapPipelineInfo(info));
      json.writeValue(pipelineOut, BeanHelper.wrapPipelineConfiguration(pipeline));
      curator.updateInfo(name, infoOut.toString(Charsets.UTF_8.name()));
      curator.updatePipeline(name, pipelineOut.toString(Charsets.UTF_8.name()));
      if (pipelineStateStore != null) {
        List<Issue> errors = new ArrayList<>();
        PipelineBeanCreator.get().create(pipeline, errors);
        pipelineStateStore.edited(user, name, tag, PipelineBeanCreator.get().getExecutionMode(pipeline, errors), false);
        pipeline.getIssues().addAll(errors);
      }

      Map<String, Object> uiInfo = extractUiInfo(pipeline);
      saveUiInfo(name, tag, uiInfo);

    } catch (Exception ex) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0204, name, ex.toString(), ex);
    }
    pipeline.setPipelineInfo(info);
    return pipeline;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<PipelineRevInfo> getHistory(String name) throws PipelineStoreException {
    return ImmutableList.of(new PipelineRevInfo(getInfo(name)));
  }

  @Override
  @SuppressWarnings("unchecked")
  public PipelineConfiguration load(String name, String tagOrRev) throws PipelineStoreException {
    if (!hasPipeline(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
    }
    try {
      String pipelineJson = curator.getPipeline(name);
      PipelineInfo info = getInfo(name);
      PipelineConfigurationJson pipelineConfigBean = json.readValue(pipelineJson, PipelineConfigurationJson.class);
      PipelineConfiguration pipeline = pipelineConfigBean.getPipelineConfiguration();
      pipeline.setPipelineInfo(info);

      Map<String, Map> uiInfo;
      String uiInfoJson = curator.getUiInfo(name);
      if (uiInfoJson != null) {
        uiInfo = json.readValue(uiInfoJson, Map.class);
        pipeline = injectUiInfo(uiInfo, pipeline);
      }

      return pipeline;
    } catch (Exception ex) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0206, name, ex.toString(), ex);
    }
  }

  @Override
  public RuleDefinitions retrieveRules(String name, String tagOrRev) throws PipelineStoreException {
    if (!pipelineToRuleDefinitionMap.containsKey(getPipelineKey(name, tagOrRev))) {
      if (!hasPipeline(name)) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
      }
      //try loading from store, needed in cases like restart
      RuleDefinitions ruleDefinitions;

      try {
        String rulesJson = curator.getRules(name);
        RuleDefinitionsJson ruleDefinitionsJsonBean = ObjectMapperFactory.get().readValue(rulesJson,
            RuleDefinitionsJson.class
        );
        ruleDefinitions = ruleDefinitionsJsonBean.getRuleDefinitions();
      } catch (IOException ex) {
        //File does not exist
        LOG.debug(ContainerError.CONTAINER_0403.getMessage(), name, ex.toString(), ex);
        ruleDefinitions = null;
      } catch (ZKStoreException e) {
        throw new FatalZKStoreException(e);
      }
      if (ruleDefinitions == null) {
        ruleDefinitions = new RuleDefinitions(new ArrayList<MetricsRuleDefinition>(),
            new ArrayList<DataRuleDefinition>(),
            new ArrayList<DriftRuleDefinition>(),
            new ArrayList<String>(),
            UUID.randomUUID()
        );
      }
      pipelineToRuleDefinitionMap.put(getPipelineKey(name, tagOrRev), ruleDefinitions);
    }
    return pipelineToRuleDefinitionMap.get(getPipelineKey(name, tagOrRev));
  }

  @Override
  public RuleDefinitions storeRules(String pipelineName, String tag, RuleDefinitions ruleDefinitions) throws
      PipelineStoreException {
    // check the uuid of the ex
    if (!hasPipeline(pipelineName)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, pipelineName);
    }
    // Listing rule definition to detect any change since the previous load.
    // two browsers could modify the same rule definition
    if (pipelineToRuleDefinitionMap.get(getPipelineKey(pipelineName, tag)) != null) {
      UUID savedUuid = pipelineToRuleDefinitionMap.get(getPipelineKey(pipelineName, tag)).getUuid();
      if (!savedUuid.equals(ruleDefinitions.getUuid())) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0205, pipelineName);
      }
    }

    UUID uuid = UUID.randomUUID();
    ruleDefinitions.setUuid(uuid);
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      ObjectMapperFactory.get().writeValue(os, BeanHelper.wrapRuleDefinitions(ruleDefinitions));
      curator.updateRules(pipelineName, os.toString(Charsets.UTF_8.name()));
      pipelineToRuleDefinitionMap.put(getPipelineKey(pipelineName, tag), ruleDefinitions);
    } catch (IOException ex) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0404, pipelineName, ex.toString(), ex);
    } catch (NotPrimaryException e) {
      LOG.error("Failed to store rules because this DataCollector is not the Primary", e);
    }
    return ruleDefinitions;
  }

  @Override
  public boolean deleteRules(String name) throws PipelineStoreException {
    pipelineToRuleDefinitionMap.remove(getPipelineKey(name, REV));
    try {
      return curator.deleteRules(name);
    } catch (ZKStoreException e) {
      LOG.error("Exception when deleting rules file", e);
    }
    return false;
  }

  public String getPipelineKey(String pipelineName, String rev) {
    return pipelineName + "$" + rev;
  }

  @Override
  public void saveUiInfo(String name, String rev, Map<String, Object> uiInfo) throws PipelineStoreException {
    try (ByteArrayOutputStream uiInfoOut = new ByteArrayOutputStream()) {
      json.writeValue(uiInfoOut, uiInfo);
      curator.updateUiInfo(name, uiInfoOut.toString(Charsets.UTF_8.name()));
    } catch (Exception ex) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0405, name, ex.toString(), ex);
    }
  }

  @VisibleForTesting
  public static Map<String, Object> extractUiInfo(PipelineConfiguration pipelineConf) {
    Map<String, Object> map = new HashMap<>();
    map.put(":pipeline:", pipelineConf.getUiInfo());
    for (StageConfiguration stage : pipelineConf.getStages()) {
      map.put(stage.getInstanceName(), stage.getUiInfo());
    }
    return map;
  }

  @SuppressWarnings("unchecked")
  PipelineConfiguration injectUiInfo(Map<String, Map> uiInfo, PipelineConfiguration pipelineConf) {
    pipelineConf.getUiInfo().clear();
    if (uiInfo.containsKey(":pipeline:")) {
      pipelineConf.getUiInfo().clear();
      pipelineConf.getUiInfo().putAll(uiInfo.get(":pipeline:"));
    }
    for (StageConfiguration stage : pipelineConf.getStages()) {
      stage.getUiInfo().clear();
      if (uiInfo.containsKey(stage.getInstanceName())) {
        stage.getUiInfo().clear();
        stage.getUiInfo().putAll(uiInfo.get(stage.getInstanceName()));
      }
    }
    return pipelineConf;
  }

  @Override
  public boolean isRemotePipeline(String name, String rev) throws PipelineStoreException {
    Object isRemote = pipelineStateStore.getState(name, rev)
        .getAttributes()
        .get(RemoteDataCollector.IS_REMOTE_PIPELINE);
    // remote attribute will be null for pipelines with version earlier than 1.3
    return isRemote != null && (boolean) isRemote;
  }

}
