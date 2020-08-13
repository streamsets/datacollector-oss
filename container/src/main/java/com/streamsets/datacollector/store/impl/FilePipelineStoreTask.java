/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.store.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineFragmentConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.creation.PipelineFragmentConfigBean;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollector;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;
import com.streamsets.datacollector.io.DataStore;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.PipelineEnvelopeJson;
import com.streamsets.datacollector.restapi.bean.PipelineInfoJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.runner.production.OffsetFileUtil;
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
import com.streamsets.datacollector.util.PipelineConfigurationUtil;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.util.credential.PipelineCredentialHandler;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.impl.PipelineUtils;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
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
  private static final String UI_INFO_FILE = "uiinfo.json";
  public static final String RULES_FILE = "rules.json";
  private static final String STATE = "state";

  private final StageLibraryTask stageLibrary;
  private final RuntimeInfo runtimeInfo;
  private final BuildInfo buildInfo;
  private Path storeDir;
  private Path samplePipelinesDir;
  private final ObjectMapper json;
  private final PipelineStateStore pipelineStateStore;
  private final ConcurrentMap<String, RuleDefinitions> pipelineToRuleDefinitionMap;
  private final EventListenerManager eventListenerManager;
  private final PipelineCreator pipelineCreator;
  private final PipelineCredentialHandler encryptingCredentialHandler;

  @Inject
  public FilePipelineStoreTask(
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      StageLibraryTask stageLibrary,
      PipelineStateStore pipelineStateStore,
      EventListenerManager eventListenerManager,
      LockCache<String> lockCache,
      PipelineCredentialHandler encryptingCredentialHandler,
      Configuration configuration
  ) {
    super("filePipelineStore");
    this.stageLibrary = stageLibrary;
    this.buildInfo = buildInfo;
    this.runtimeInfo = runtimeInfo;
    json = ObjectMapperFactory.get();
    pipelineToRuleDefinitionMap = new ConcurrentHashMap<>();
    this.pipelineStateStore = pipelineStateStore;
    this.lockCache = lockCache;
    pipelineCreator = new PipelineCreator(
        stageLibrary.getPipeline(),
        SCHEMA_VERSION,
        buildInfo.getVersion(),
        runtimeInfo.getId(),
        this::getDefaultStatsAggrStageInstance,
        this::getDefaultTestOriginStageInstance,
        this::getDefaultErrorStageInstance
    );
    this.eventListenerManager = eventListenerManager;
    this.encryptingCredentialHandler = encryptingCredentialHandler;
    PipelineBeanCreator.prepareForConnections(configuration, runtimeInfo);
  }

  @VisibleForTesting
  Path getStoreDir() {
    return storeDir;
  }

  public void registerStateListener(StateEventListener stateListener) {
    eventListenerManager.addStateEventListener(stateListener);
  }

  @Override
  public void initTask() {
    storeDir = Paths.get(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_INFO_BASE_DIR);
    samplePipelinesDir = Paths.get(runtimeInfo.getSamplePipelinesDir());
    if (!Files.exists(storeDir)) {
      try {
        Files.createDirectories(storeDir);
      } catch (IOException e) {
        throw new RuntimeException(Utils.format("Could not create directory '{}'", storeDir), e);
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

  public Path getPipelineDir(String name) {
    return storeDir.resolve(PipelineUtils.escapedPipelineName(name));
  }

  @VisibleForTesting
  private Path getInfoFile(String name) {
    return getPipelineDir(name).resolve(INFO_FILE);
  }

  private Path getPipelineFile(String name) {
    return getPipelineDir(name).resolve(PIPELINE_FILE);
  }

  private Path getPipelineUiInfoFile(String name) {
    return getPipelineDir(name).resolve(UI_INFO_FILE);
  }

  private Path getRulesFile(String name) {
    return getPipelineDir(name).resolve(RULES_FILE);
  }

  @Override
  public boolean hasPipeline(String name) {
    return Files.exists(getPipelineDir(name));
  }

  @Override
  public PipelineConfiguration create(
      String user,
      String pipelineId,
      String pipelineTitle,
      String description,
      boolean isRemote,
      boolean draft,
      Map<String, Object> metadata
  ) throws PipelineStoreException {
    synchronized (lockCache.getLock(pipelineId)) {
      if (hasPipeline(pipelineId)) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0201, pipelineId);
      }

      if (!draft) {
        try {
          Files.createDirectory(getPipelineDir(pipelineId));
          Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineId, REV).toPath());
        } catch (IOException e) {
          throw new PipelineStoreException(
              ContainerError.CONTAINER_0202,
              pipelineId,
              "mkdir failed",
              e
          );
        }
      }

      PipelineConfiguration pipeline = pipelineCreator.create(user, pipelineId, pipelineTitle, description, new Date());

      if (!draft) {
        DataStore dataStorePipeline = new DataStore(getPipelineFile(pipelineId).toFile());
        DataStore dataStoreInfo = new DataStore(getInfoFile(pipelineId).toFile());
        try (
            OutputStream pipelineFile = dataStorePipeline.getOutputStream();
            OutputStream infoFile = dataStoreInfo.getOutputStream()
        ){
          // it is important to always modify pipeline.json before modifying info.json in order for recovery to work
          json.writeValue(pipelineFile, BeanHelper.wrapPipelineConfiguration(pipeline));
          dataStorePipeline.commit(pipelineFile);
          json.writeValue(infoFile, BeanHelper.wrapPipelineInfo(pipeline.getInfo()));
          dataStoreInfo.commit(infoFile);
        } catch (Exception ex) {
          throw new PipelineStoreException(ContainerError.CONTAINER_0202, pipelineId, ex.toString(), ex);
        } finally {
          dataStorePipeline.release();
          dataStoreInfo.release();
        }
        if (pipelineStateStore != null) {
          pipelineStateStore.edited(user, pipelineId, REV, ExecutionMode.STANDALONE, isRemote, metadata);
        }
      }

      pipeline.setPipelineInfo(pipeline.getInfo());
      return pipeline;
    }
  }

  private boolean cleanUp(String name) {
    LogUtil.resetRollingFileAppender(name, "0", STATE);
    boolean deleted = PipelineDirectoryUtil.deleteAll(getPipelineDir(name).toFile());
    deleted &= PipelineDirectoryUtil.deletePipelineDir(runtimeInfo, name);
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
        PipelineState currentState = pipelineStateStore.getState(name, REV);
        PipelineStatus pipelineStatus = currentState.getStatus();
        if (pipelineStatus.isActive()) {
          throw new PipelineStoreException(ContainerError.CONTAINER_0208, pipelineStatus);
        }
        Map<String, String> offset = OffsetFileUtil.getOffsets(runtimeInfo, name, REV);
        if (!cleanUp(name)) {
          throw new PipelineStoreException(ContainerError.CONTAINER_0203, name);
        }
        PipelineState latestState = new PipelineStateImpl(
            currentState.getUser(),
            currentState.getPipelineId(),
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
          if (eventListenerManager != null) {
            eventListenerManager.broadcastStateChange(currentState, latestState, null, offset);
          }
        } catch (Exception e) {
          LOG.warn("Cannot set delete event for pipeline");
        }
        pipelineStateStore.delete(name, REV);
      }
    }
  }

  private DirectoryStream.Filter<Path> filterHiddenFiles = path -> !path.getFileName().toString().startsWith(".");

  @Override
  public List<PipelineInfo> getPipelines() throws PipelineStoreException {
    List<PipelineInfo> pipelineInfoList = new ArrayList<>();

    List<String> fileNames = new ArrayList<>();
    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(storeDir, filterHiddenFiles)) {
      for (Path path : directoryStream) {
        fileNames.add(path.getFileName().toString());
      }
    } catch (IOException ex) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0213, storeDir, ex);
    }

    for (String name : fileNames) {
      PipelineInfoJson pipelineInfoJsonBean;
      DataStore dataStoreInfo = new DataStore(getInfoFile(name).toFile());
      syncPipelineWithPipelineInfoIfNeeded(dataStoreInfo, name);
      try (InputStream infoFile = dataStoreInfo.getInputStream()){
        pipelineInfoJsonBean = json.readValue(infoFile, PipelineInfoJson.class);
      } catch (IOException e) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0206, name, e);
      }
      pipelineInfoList.add(pipelineInfoJsonBean.getPipelineInfo());
    }
    return Collections.unmodifiableList(pipelineInfoList);
  }

  private void syncPipelineWithPipelineInfoIfNeeded(DataStore dataStoreInfo, String name)
      throws PipelineStoreException {
    try {
      boolean syncNeeded = false;
      if (!dataStoreInfo.exists()) {
        syncNeeded = true;
      } else {
        try (InputStream infoFile = dataStoreInfo.getInputStream()) {
          if (dataStoreInfo.isRecovered()) {
            syncNeeded = true;
          }
        }
      }
      if (syncNeeded) {
        syncPipelineWithPipelineInfo(name);
      }
    } catch (IOException e) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0206, name, e);
    }
  }

  private void syncPipelineWithPipelineInfo(String name) throws IOException {
    DataStore dataStorePipeline = new DataStore(getPipelineFile(name).toFile());
    try (InputStream pipelineFile = dataStorePipeline.getInputStream()) {
      PipelineConfigurationJson pipelineConfigBean =
          json.readValue(pipelineFile, PipelineConfigurationJson.class);
      DataStore dataStoreInfo = new DataStore(getInfoFile(name).toFile());
      try (OutputStream infoFile = dataStoreInfo.getOutputStream()) {
        json.writeValue(infoFile, BeanHelper.wrapPipelineInfo(pipelineConfigBean.getInfo().getPipelineInfo()));
        dataStoreInfo.commit(infoFile);
      } finally {
        dataStoreInfo.release();
      }
    }
  }

  @Override
  public PipelineInfo getInfo(String name) throws PipelineStoreException {
    synchronized (lockCache.getLock(name)) {
      if (!hasPipeline(name)) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
      }
      DataStore dataStoreInfo = new DataStore(getInfoFile(name).toFile());
      syncPipelineWithPipelineInfoIfNeeded(dataStoreInfo, name);
      try (InputStream infoFile = dataStoreInfo.getInputStream()) {
        PipelineInfoJson pipelineInfoJsonBean = json.readValue(infoFile, PipelineInfoJson.class);
        return pipelineInfoJsonBean.getPipelineInfo();
      } catch (Exception ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0206, name, ex);
      }
    }
  }

  @Override
  public PipelineConfiguration save(
      String user,
      String name,
      String tag,
      String tagDescription,
      PipelineConfiguration pipeline,
      boolean encryptCredentials
  ) throws PipelineStoreException {
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
      PipelineInfo info = new PipelineInfo(
          getInfo(name),
          pipeline.getTitle(),
          pipeline.getDescription(),
          new Date(),
          user,
          REV,
          uuid,
          pipeline.isValid(),
          pipeline.getMetadata(),
          pipeline.getInfo().getSdcVersion(),
          runtimeInfo.getId()
      );
      if (encryptCredentials) {
        encryptingCredentialHandler.handlePipelineConfigCredentials(pipeline);
      }
      DataStore dataStorePipeline = new DataStore(getPipelineFile(name).toFile());
      DataStore dataStoreInfo = new DataStore(getInfoFile(name).toFile());
      try (
          OutputStream pipelineFile = dataStorePipeline.getOutputStream();
          OutputStream infoFile = dataStoreInfo.getOutputStream()
        ){
        pipeline.setUuid(uuid);
        pipeline.setInfo(info);
        json.writeValue(pipelineFile, BeanHelper.wrapPipelineConfiguration(pipeline));
        dataStorePipeline.commit(pipelineFile);
        json.writeValue(infoFile, BeanHelper.wrapPipelineInfo(info));
        dataStoreInfo.commit(infoFile);
        if (pipelineStateStore != null) {
          List<Issue> errors = new ArrayList<>();
          PipelineBeanCreator.get().create(pipeline, errors, null, user, new HashMap<>());
          pipelineStateStore.edited(user,
              name,
              tag,
              PipelineBeanCreator.get().getExecutionMode(pipeline, errors),
              false,
              null
          );
          pipeline.getIssues().addAll(errors);
        }

        Map<String, Object> uiInfo = extractUiInfo(pipeline);
        saveUiInfo(name, tag, uiInfo);

      } catch (Exception ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0204, name, ex.toString(), ex);
      } finally {
        dataStorePipeline.release();
        dataStoreInfo.release();
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
      DataStore dataStorePipeline = new DataStore(getPipelineFile(name).toFile());
      try {
        boolean syncNeeded = false;
        try (InputStream pipelineFile = dataStorePipeline.getInputStream()) {
          if (dataStorePipeline.isRecovered()) {
            syncNeeded = true;
          }
        }
        if (syncNeeded) {
          syncPipelineWithPipelineInfo(name);
        }
      } catch (IOException ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0206, name, ex.toString(), ex);
      }
      PipelineInfo info = getInfo(name);
      try (InputStream pipelineFile = dataStorePipeline.getInputStream()) {
        PipelineConfigurationJson pipelineConfigBean =
          json.readValue(pipelineFile, PipelineConfigurationJson.class);
        PipelineConfiguration pipeline = pipelineConfigBean.getPipelineConfiguration();
        pipeline.setPipelineInfo(info);

        Map<String, Map> uiInfo;
        if (Files.exists(getPipelineUiInfoFile(name))) {
          try (InputStream uiInfoFile = Files.newInputStream(getPipelineUiInfoFile(name))) {
            uiInfo = json.readValue(uiInfoFile, Map.class);
            pipeline = injectUiInfo(uiInfo, pipeline);
          }
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
        DataStore ds = new DataStore(getRulesFile(name).toFile());
        try {
          if (ds.exists()) {
            try (InputStream is = ds.getInputStream()) {
              RuleDefinitionsJson ruleDefinitionsJsonBean =
                  ObjectMapperFactory.get().readValue(is, RuleDefinitionsJson.class);
              ruleDefinitions = ruleDefinitionsJsonBean.getRuleDefinitions();
            }
          }
        } catch (IOException ex) {
          //File does not exist
          LOG.debug(ContainerError.CONTAINER_0403.getMessage(), name, ex.toString(),
              ex);
          ruleDefinitions = null;
        }
        if(ruleDefinitions == null) {
          ruleDefinitions = new RuleDefinitions(
              PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
              RuleDefinitionsConfigBean.VERSION,
              new ArrayList<>(),
              new ArrayList<>(),
              new ArrayList<>(),
              new ArrayList<>(),
              UUID.randomUUID(),
              stageLibrary.getPipelineRules().getPipelineRulesDefaultConfigs()
          );
        }
        pipelineToRuleDefinitionMap.put(getPipelineKey(name, tagOrRev), ruleDefinitions);
      }
      return pipelineToRuleDefinitionMap.get(getPipelineKey(name, tagOrRev));
    }
  }

  @Override
  public RuleDefinitions storeRules(
      String pipelineName,
      String tag,
      RuleDefinitions ruleDefinitions,
      boolean draft
  )
    throws PipelineStoreException {
    synchronized (lockCache.getLock(pipelineName)) {
      if (!draft) {
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
      }

      UUID uuid = UUID.randomUUID();
      ruleDefinitions.setUuid(uuid);

      if (!draft) {
        DataStore dataStore = new DataStore(getRulesFile(pipelineName).toFile());
        try (OutputStream os = dataStore.getOutputStream()) {
          ObjectMapperFactory.get().writeValue(os, BeanHelper.wrapRuleDefinitions(ruleDefinitions));
          dataStore.commit(os);
          pipelineToRuleDefinitionMap.put(getPipelineKey(pipelineName, tag), ruleDefinitions);
        } catch (IOException ex) {
          throw new PipelineStoreException(ContainerError.CONTAINER_0404, pipelineName, ex.toString(), ex);
        } finally {
          dataStore.release();
        }
      }

      return ruleDefinitions;
    }
  }

  @Override
  public boolean deleteRules(String name) {
    synchronized (lockCache.getLock(name)) {
      pipelineToRuleDefinitionMap.remove(getPipelineKey(name, REV));
      if (hasPipeline(name)) {
        try {
          return Files.deleteIfExists(getRulesFile(name));
        } catch (IOException e) {
          LOG.error("Exception when deleting rules file", e);
          return false;
        }
      }
      return false;
    }
  }

  private String getPipelineKey(String pipelineName, String rev) {
    return pipelineName + "$" + rev;
  }

  @Override
  public void saveUiInfo(String name, String rev, Map<String, Object> uiInfo) throws PipelineStoreException {
    try (OutputStream uiInfoFile = Files.newOutputStream(getPipelineUiInfoFile(name))){
      json.writeValue(uiInfoFile, uiInfo);
    } catch (Exception ex) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0405, name, ex.toString(), ex);
    }
  }

  @Override
  public PipelineConfiguration saveMetadata(
      String user,
      String name,
      String rev,
      Map<String, Object> metadata
  ) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      if (!hasPipeline(name)) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
      }
      PipelineConfiguration savedPipeline = load(name, rev);
      PipelineInfo savedInfo = getInfo(name);

      if (pipelineStateStore != null) {
        PipelineStatus pipelineStatus = pipelineStateStore.getState(name, rev).getStatus();
        if (pipelineStatus.isActive()) {
          throw new PipelineStoreException(ContainerError.CONTAINER_0208, pipelineStatus);
        }
      }

      PipelineInfo updatedInfo = new PipelineInfo(
          getInfo(name),
          savedPipeline.getTitle(),
          savedPipeline.getDescription(),
          new Date(),
          user,
          REV,
          savedInfo.getUuid(),
          savedInfo.isValid(),
          metadata,
          buildInfo.getVersion(),
          runtimeInfo.getId()
      );
      savedPipeline.setMetadata(metadata);
      savedPipeline.setPipelineInfo(updatedInfo);

      DataStore dataStorePipeline = new DataStore(getPipelineFile(name).toFile());
      DataStore dataStoreInfo = new DataStore(getInfoFile(name).toFile());
      try (
          OutputStream pipelineFile = dataStorePipeline.getOutputStream();
          OutputStream infoFile = dataStoreInfo.getOutputStream()
      ) {
        json.writeValue(pipelineFile, BeanHelper.wrapPipelineConfiguration(savedPipeline));
        dataStorePipeline.commit(pipelineFile);
        json.writeValue(infoFile, BeanHelper.wrapPipelineInfo(updatedInfo));
        dataStoreInfo.commit(infoFile);
      } catch (Exception ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0204, name, ex.toString(), ex);
      } finally {
        dataStorePipeline.release();
        dataStoreInfo.release();
      }
      return savedPipeline;
    }
  }

  @VisibleForTesting
  static Map<String, Object> extractUiInfo(PipelineConfiguration pipelineConf) {
    Map<String, Object> map = new HashMap<>();
    map.put(":pipeline:", pipelineConf.getUiInfo());
    for (StageConfiguration stage : pipelineConf.getStages()) {
      map.put(stage.getInstanceName(), stage.getUiInfo());
    }
    return map;
  }

  @SuppressWarnings("unchecked")
  private PipelineConfiguration injectUiInfo(Map<String, Map> uiInfo, PipelineConfiguration pipelineConf) {
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
    Object isRemote = pipelineStateStore
        .getState(name, rev)
        .getAttributes()
        .get(RemoteDataCollector.IS_REMOTE_PIPELINE);
    // remote attribute will be null for pipelines with version earlier than 1.3
    return isRemote != null && (boolean) isRemote;
  }

  @Override
  public PipelineFragmentConfiguration createPipelineFragment(
      String user,
      String pipelineId,
      String pipelineTitle,
      String description,
      boolean draft
  ) {
    // Supporting only draft version now - not storing in disk
    synchronized (lockCache.getLock(pipelineId)) {
      Date date = new Date();
      UUID uuid = UUID.randomUUID();
      PipelineInfo info = new PipelineInfo(
          pipelineId,
          pipelineTitle,
          description,
          date,
          date,
          user,
          user,
          REV,
          uuid,
          false,
          null,
          buildInfo.getVersion(),
          runtimeInfo.getId()
      );

      PipelineFragmentConfiguration pipelineFragmentConfiguration = new PipelineFragmentConfiguration(
          uuid,
          PipelineFragmentConfigBean.VERSION,
          FRAGMENT_SCHEMA_VERSION,
          pipelineTitle,
          pipelineId,
          pipelineId,
          description,
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyMap(),
          stageLibrary.getPipelineFragment().getPipelineFragmentDefaultConfigs(),
          getDefaultTestOriginStageInstance()
      );
      pipelineFragmentConfiguration.setPipelineInfo(info);
      return pipelineFragmentConfiguration;
    }
  }

  private StageConfiguration getDefaultTestOriginStageInstance() {
    StageConfiguration testOriginStageInstance = PipelineConfigurationUtil.getStageConfigurationWithDefaultValues(
        stageLibrary,
        PipelineConfigBean.DEFAULT_TEST_ORIGIN_LIBRARY_NAME,
        PipelineConfigBean.DEFAULT_TEST_ORIGIN_STAGE_NAME,
        PipelineConfigBean.DEFAULT_TEST_ORIGIN_STAGE_NAME + "_TestOriginStage",
        "Test Origin - "
    );
    if (testOriginStageInstance != null) {
      testOriginStageInstance.setOutputLanes(
          ImmutableList.of(testOriginStageInstance.getInstanceName() + "OutputLane1")
      );
    }
    return testOriginStageInstance;
  }

  private StageConfiguration getDefaultStatsAggrStageInstance() {
    return PipelineConfigurationUtil.getStageConfigurationWithDefaultValues(
        stageLibrary,
        PipelineConfigBean.DEFAULT_STATS_AGGREGATOR_LIBRARY_NAME,
        PipelineConfigBean.DEFAULT_STATS_AGGREGATOR_STAGE_NAME,
         "statsAggregatorStageInstance",
        "Stats Aggregator -"
    );
  }

  private StageConfiguration getDefaultErrorStageInstance() {
    return PipelineConfigurationUtil.getStageConfigurationWithDefaultValues(
        stageLibrary,
        PipelineConfigBean.TRASH_LIBRARY_NAME,
        PipelineConfigBean.TRASH_STAGE_NAME,
        "errorStageStageInstance",
        "Error -"
    );
  }

  @Override
  public List<PipelineInfo> getSamplePipelines() throws PipelineStoreException {
    if (samplePipelinesDir == null || !Files.exists(samplePipelinesDir)) {
      return Collections.emptyList();
    }
    List<PipelineInfo> pipelineInfoList = new ArrayList<>();
    List<Path> fileNames = new ArrayList<>();
    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(samplePipelinesDir, filterHiddenFiles)) {
      for (Path path : directoryStream) {
        fileNames.add(path);
      }
    } catch (IOException ex) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0213, samplePipelinesDir, ex);
    }

    for (Path fileName : fileNames) {
      PipelineEnvelopeJson pipelineEnvelopeJson;
      DataStore dataStoreInfo = new DataStore(fileName.toFile());
      try (InputStream pipelineEnvelopeFile = dataStoreInfo.getInputStream()){
        pipelineEnvelopeJson = json.readValue(pipelineEnvelopeFile, PipelineEnvelopeJson.class);
        PipelineInfo pipelineInfo = BeanHelper.unwrapPipelineInfo(pipelineEnvelopeJson.getPipelineConfig().getInfo());

        // Override sample pipeline Id using the file name, so loadSamplePipeline works without any issue even if
        // pipelineId inside the JSON doesn't match the file name.
        pipelineInfo.setPipelineId(fileName.getFileName().toString().replace(".json", ""));
        pipelineInfoList.add(pipelineInfo);
      } catch (IOException e) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0216, fileName, e);
      }
    }
    return Collections.unmodifiableList(pipelineInfoList);
  }

  @Override
  public PipelineEnvelopeJson loadSamplePipeline(String samplePipelineId) throws PipelineStoreException {
    synchronized (lockCache.getLock(samplePipelineId)) {
      if (!hasSamplePipeline(samplePipelineId)) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0215, samplePipelineId);
      }
      DataStore dataStorePipeline = new DataStore(getSamplePipelineFile(samplePipelineId).toFile());
      try (InputStream pipelineFile = dataStorePipeline.getInputStream()) {
        return json.readValue(pipelineFile, PipelineEnvelopeJson.class);
      }
      catch (Exception ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0216, samplePipelineId, ex.toString(), ex);
      }
    }
  }

  public boolean hasSamplePipeline(String pipelineId) {
    return samplePipelinesDir != null && Files.exists(getSamplePipelineFile(pipelineId));
  }

  public Path getSamplePipelineFile(String pipelineId) {
    return samplePipelinesDir.resolve(pipelineId + ".json");
  }

}
