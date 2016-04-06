/**
 * Copyright 2015 StreamSets Inc.
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
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.util.LogUtil;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.impl.PipelineUtils;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollector;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
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
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;
import com.streamsets.datacollector.validation.Issue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class FilePipelineStoreTask extends AbstractTask implements PipelineStoreTask {
  private static final Logger LOG = LoggerFactory.getLogger(FilePipelineStoreTask.class);
  private final LockCache<String> lockCache;
  @VisibleForTesting
  static final String REV = "0";
  public static final String INFO_FILE = "info.json";
  public static final String PIPELINE_FILE = "pipeline.json";
  public static final String UI_INFO_FILE = "uiinfo.json";
  public static final String RULES_FILE = "rules.json";
  public static final String STATE = "state";

  private final StageLibraryTask stageLibrary;
  private final RuntimeInfo runtimeInfo;
  private File storeDir;
  private final ObjectMapper json;
  private final PipelineStateStore pipelineStateStore;
  private final ConcurrentMap<String, RuleDefinitions> pipelineToRuleDefinitionMap;

  @Inject
  public FilePipelineStoreTask(RuntimeInfo runtimeInfo, StageLibraryTask stageLibrary,
    PipelineStateStore pipelineStateStore, LockCache<String> lockCache) {
    super("filePipelineStore");
    this.stageLibrary = stageLibrary;
    this.runtimeInfo = runtimeInfo;
    json = ObjectMapperFactory.get();
    pipelineToRuleDefinitionMap = new ConcurrentHashMap<>();
    this.pipelineStateStore = pipelineStateStore;
    this.lockCache = lockCache;
  }

  @VisibleForTesting
  File getStoreDir() {
    return storeDir;
  }

  @Override
  public void initTask() {
    storeDir = new File(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_INFO_BASE_DIR);
    if (!storeDir.exists()) {
      if (!storeDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create directory '{}'", storeDir.getAbsolutePath()));
      }
    }
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

  public File getPipelineDir(String name) {
    return new File(storeDir, PipelineUtils.escapedPipelineName(name));
  }

  @VisibleForTesting
  File getInfoFile(String name) {
    return new File(getPipelineDir(name), INFO_FILE);
  }

  private File getPipelineFile(String name) {
    return new File(getPipelineDir(name), PIPELINE_FILE);
  }

  private File getPipelineUiInfoFile(String name) {
    return new File(getPipelineDir(name), UI_INFO_FILE);
  }

  private File getRulesFile(String name) {
    return new File(getPipelineDir(name), RULES_FILE);
  }

  @Override
  public boolean hasPipeline(String name) {
    return getPipelineDir(name).exists();
  }

  @Override
  public PipelineConfiguration create(String user, String name, String description, boolean isRemote) throws PipelineStoreException {
    synchronized (lockCache.getLock(name)) {
      if (hasPipeline(name)) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0201, name);
      }
      if (!getPipelineDir(name).mkdir()) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0202, name, Utils.format("'{}' mkdir failed",
          getPipelineDir(name)));
      }

      Date date = new Date();
      UUID uuid = UUID.randomUUID();
      PipelineInfo info = new PipelineInfo(name, description, date, date, user, user, REV, uuid, false);
      PipelineConfiguration pipeline =
        new PipelineConfiguration(SCHEMA_VERSION, PipelineConfigBean.VERSION, uuid, description, stageLibrary
          .getPipeline().getPipelineDefaultConfigs(), Collections.EMPTY_MAP, Collections.EMPTY_LIST, null, null);

      try {
        json.writeValue(getInfoFile(name), BeanHelper.wrapPipelineInfo(info));
        json.writeValue(getPipelineFile(name), BeanHelper.wrapPipelineConfiguration(pipeline));
      } catch (Exception ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0202, name, ex.toString(), ex);
      }
      pipeline.setPipelineInfo(info);
      if (pipelineStateStore != null) {
        pipelineStateStore.edited(user, name, REV, ExecutionMode.STANDALONE, isRemote);
      }
      return pipeline;
    }
  }

  private boolean cleanUp(String name) throws PipelineStoreException {
    boolean deleted = PipelineDirectoryUtil.deleteAll(getPipelineDir(name));
    deleted &= PipelineDirectoryUtil.deletePipelineDir(runtimeInfo, name);
    if(deleted) {
      LogUtil.resetRollingFileAppender(name, "0", STATE);
    }
    return deleted;
  }

  @Override
  public void delete(String name) throws PipelineStoreException {
    synchronized (lockCache.getLock(name)) {
      if (!hasPipeline(name)) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
      }
      if (pipelineStateStore != null) {
        // For now, passing rev 0 - make delete take tag/rev as a parameter
        PipelineStatus pipelineStatus = pipelineStateStore.getState(name, REV).getStatus();
        if (pipelineStatus.isActive()) {
          throw new PipelineStoreException(ContainerError.CONTAINER_0208, pipelineStatus);
        }
        if (!cleanUp(name)) {
          throw new PipelineStoreException(ContainerError.CONTAINER_0203, name);
        }
      }
    }
  }

  @Override
  public List<PipelineInfo> getPipelines() throws PipelineStoreException {
    List<PipelineInfo> pipelineInfoList = new ArrayList<PipelineInfo>();
    String[] filenames = storeDir.list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        // If one browses to the pipelines directory, mac creates a ".DS_store directory and this causes us problems
        // So filter it out
        return !name.startsWith(".");
      }

    });
    // filenames can be null if storeDir is not actually a directory.
    if (filenames == null) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0213, storeDir.getPath());
    }

    for (String name : filenames) {
      PipelineInfoJson pipelineInfoJsonBean;
      try {
        pipelineInfoJsonBean = json.readValue(getInfoFile(name), PipelineInfoJson.class);
      } catch (IOException e) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0206, name);
      }
      pipelineInfoList.add(pipelineInfoJsonBean.getPipelineInfo());
    }
    return Collections.unmodifiableList(pipelineInfoList);
  }

  @Override
  public PipelineInfo getInfo(String name) throws PipelineStoreException {
    return getInfo(name, false);
  }

  private PipelineInfo getInfo(String name, boolean checkExistence) throws PipelineStoreException {
    synchronized (lockCache.getLock(name)) {
      if (checkExistence && !hasPipeline(name)) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
      }
      try {
        PipelineInfoJson pipelineInfoJsonBean = json.readValue(getInfoFile(name), PipelineInfoJson.class);
        return pipelineInfoJsonBean.getPipelineInfo();
      } catch (Exception ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0206, name);
      }
    }
  }

  @Override
  public PipelineConfiguration save(String user, String name, String tag, String tagDescription,
    PipelineConfiguration pipeline) throws PipelineStoreException {
    synchronized (lockCache.getLock(name)) {
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
      PipelineInfo info =
        new PipelineInfo(getInfo(name), pipeline.getDescription(), new Date(), user, REV, uuid, pipeline.isValid());
      try {
        pipeline.setUuid(uuid);
        json.writeValue(getInfoFile(name), BeanHelper.wrapPipelineInfo(info));
        json.writeValue(getPipelineFile(name), BeanHelper.wrapPipelineConfiguration(pipeline));
        if (pipelineStateStore != null) {
          List<Issue> errors = new ArrayList<>();
          PipelineBeanCreator.get().create(pipeline, errors);
          pipelineStateStore.edited(user, name, tag,  PipelineBeanCreator.get().getExecutionMode(pipeline, errors), false);
          pipeline.getIssues().addAll(errors);
        }

        Map uiInfo = extractUiInfo(pipeline);
        saveUiInfo(name, tag, uiInfo);

      } catch (Exception ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0204, name, ex.toString(), ex);
      }
      pipeline.setPipelineInfo(info);
      return pipeline;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<PipelineRevInfo> getHistory(String name) throws PipelineStoreException {
    synchronized (lockCache.getLock(name)) {
      return ImmutableList.of(new PipelineRevInfo(getInfo(name)));
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public PipelineConfiguration load(String name, String tagOrRev) throws PipelineStoreException {
    synchronized (lockCache.getLock(name)) {
      if (!hasPipeline(name)) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
      }
      try {
        PipelineInfo info = getInfo(name);
        PipelineConfigurationJson pipelineConfigBean =
          json.readValue(getPipelineFile(name), PipelineConfigurationJson.class);
        PipelineConfiguration pipeline = pipelineConfigBean.getPipelineConfiguration();
        pipeline.setPipelineInfo(info);

        Map<String, Map> uiInfo;
        if (getPipelineUiInfoFile(name).exists()) {
          uiInfo = json.readValue(getPipelineUiInfoFile(name), Map.class);
          pipeline = injectUiInfo(uiInfo, pipeline);
        }

        return pipeline;
      }
      catch (Exception ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0206, name, ex.toString(), ex);
      }
    }
  }

  @Override
  public RuleDefinitions retrieveRules(String name, String tagOrRev) throws PipelineStoreException {
    synchronized (lockCache.getLock(name)) {
      if(!pipelineToRuleDefinitionMap.containsKey(getPipelineKey(name, tagOrRev))) {
        if (!hasPipeline(name)) {
          throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
        }
        //try loading from store, needed in cases like restart
        RuleDefinitions ruleDefinitions = null;
        File rulesFile = getRulesFile(name);
        if(rulesFile.exists()) {
          try (InputStream is = new DataStore(rulesFile).getInputStream()){
            RuleDefinitionsJson ruleDefinitionsJsonBean =
              ObjectMapperFactory.get().readValue(is, RuleDefinitionsJson.class);
            ruleDefinitions = ruleDefinitionsJsonBean.getRuleDefinitions();
          } catch (IOException ex) {
            //File does not exist
            LOG.debug(ContainerError.CONTAINER_0403.getMessage(), name, ex.toString(),
                      ex);
            ruleDefinitions = null;
          }
        }
        if(ruleDefinitions == null) {
          ruleDefinitions = new RuleDefinitions(new ArrayList<MetricsRuleDefinition>(),
            new ArrayList<DataRuleDefinition>(), new ArrayList<DriftRuleDefinition>(),
              new ArrayList<String>(), UUID.randomUUID());
        }
        pipelineToRuleDefinitionMap.put(getPipelineKey(name, tagOrRev), ruleDefinitions);
      }
      return pipelineToRuleDefinitionMap.get(getPipelineKey(name, tagOrRev));
    }
  }

  @Override
  public RuleDefinitions storeRules(String pipelineName, String tag, RuleDefinitions ruleDefinitions)
    throws PipelineStoreException {
    synchronized (lockCache.getLock(pipelineName)) {
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
      DataStore dataStore = new DataStore(getRulesFile(pipelineName));
      try (OutputStream os = dataStore.getOutputStream()) {
        ObjectMapperFactory.get().writeValue(os, BeanHelper.wrapRuleDefinitions(ruleDefinitions));
        dataStore.commit(os);
        pipelineToRuleDefinitionMap.put(getPipelineKey(pipelineName, tag), ruleDefinitions);
      } catch (IOException ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0404, pipelineName, ex.toString(), ex);
      } finally {
        dataStore.release();
      }
      return ruleDefinitions;
    }
  }

  @Override
  public boolean deleteRules(String name) throws PipelineStoreException {
    synchronized (lockCache.getLock(name)) {
      pipelineToRuleDefinitionMap.remove(getPipelineKey(name, REV));

      if (hasPipeline(name) && getRulesFile(name) != null && getRulesFile(name).exists()) {
        return getRulesFile(name).delete();
      }
      return false;
    }
  }

  public String getPipelineKey(String pipelineName, String rev) {
    return pipelineName + "$" + rev;
  }

  @Override
  public void saveUiInfo(String name, String rev, Map<String, Object> uiInfo) throws PipelineStoreException {
    try {
      json.writeValue(getPipelineUiInfoFile(name), uiInfo);
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
    Object isRemote = pipelineStateStore.getState(name, rev).getAttributes().get(RemoteDataCollector.IS_REMOTE_PIPELINE);
    // remote attribute will be null for pipelines with version earlier than 1.3
    return (isRemote == null) ? false : (boolean) isRemote;
  }

}
