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
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollector;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;
import com.streamsets.datacollector.io.DataStore;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.DataCollectorBuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.PipelineInfoJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.runner.production.OffsetFileUtil;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineRevInfo;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.LogUtil;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;
import com.streamsets.datacollector.util.PipelineException;
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
  public static final String UI_INFO_FILE = "uiinfo.json";
  public static final String RULES_FILE = "rules.json";
  public static final String STATE = "state";

  private final StageLibraryTask stageLibrary;
  private final RuntimeInfo runtimeInfo;
  private final BuildInfo buildInfo;
  private Path storeDir;
  private final ObjectMapper json;
  private final PipelineStateStore pipelineStateStore;
  private final ConcurrentMap<String, RuleDefinitions> pipelineToRuleDefinitionMap;
  private StateEventListener stateEventListener;

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
    buildInfo = new DataCollectorBuildInfo();
  }

  @VisibleForTesting
  Path getStoreDir() {
    return storeDir;
  }

  public void registerStateListener(StateEventListener stateListener) {
    stateEventListener = stateListener;
  }

  @Override
  public void initTask() {
    storeDir = Paths.get(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_INFO_BASE_DIR);
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
  Path getInfoFile(String name) {
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
      boolean draft
  ) throws PipelineStoreException {
    synchronized (lockCache.getLock(pipelineId)) {
      if (hasPipeline(pipelineId)) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0201, pipelineId);
      }

      if (!draft) {
        try {
          Files.createDirectory(getPipelineDir(pipelineId));
        } catch (IOException e) {
          throw new PipelineStoreException(
              ContainerError.CONTAINER_0202,
              pipelineId,
              Utils.format("'{}' mkdir failed", getPipelineDir(pipelineId)),
              e
          );
        }
      }

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

      PipelineConfiguration pipeline = new PipelineConfiguration(
          SCHEMA_VERSION,
          PipelineConfigBean.VERSION,
          pipelineId,
          uuid,
          pipelineTitle,
          description,
          stageLibrary.getPipeline().getPipelineDefaultConfigs(),
          Collections.<String, Object>emptyMap(),
          Collections.<StageConfiguration>emptyList(),
          null,
          null,
          Collections.emptyList(),
          Collections.emptyList()
      );

      if (!draft) {
        try (
            OutputStream infoFile = Files.newOutputStream(getInfoFile(pipelineId));
            OutputStream pipelineFile = Files.newOutputStream(getPipelineFile(pipelineId));
        ){
          json.writeValue(infoFile, BeanHelper.wrapPipelineInfo(info));
          json.writeValue(pipelineFile, BeanHelper.wrapPipelineConfiguration(pipeline));
        } catch (Exception ex) {
          throw new PipelineStoreException(ContainerError.CONTAINER_0202, pipelineId, ex.toString(), ex);
        }
        if (pipelineStateStore != null) {
          pipelineStateStore.edited(user, pipelineId, REV, ExecutionMode.STANDALONE, isRemote);
        }
      }

      pipeline.setPipelineInfo(info);
      return pipeline;
    }
  }

  private boolean cleanUp(String name) throws PipelineStoreException {
    boolean deleted = PipelineDirectoryUtil.deleteAll(getPipelineDir(name).toFile());
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
          if (stateEventListener != null) {
            stateEventListener.onStateChange(currentState, latestState, "", null, offset);
          }
        } catch (Exception e) {
          LOG.warn("Cannot set delete event for pipeline");
        }
      }
    }
  }

  DirectoryStream.Filter<Path> filterHiddenFiles = new DirectoryStream.Filter<Path>() {
    public boolean accept(Path path) throws IOException {
      return !path.getFileName().toString().startsWith(".");
    }
  };

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
      try (InputStream infoFile = Files.newInputStream(getInfoFile(name))){
        pipelineInfoJsonBean = json.readValue(infoFile, PipelineInfoJson.class);
      } catch (IOException e) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0206, name, e);
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
      try (InputStream infoFile = Files.newInputStream(getInfoFile(name))) {
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
      PipelineConfiguration pipeline
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
          buildInfo.getVersion(),
          runtimeInfo.getId()
      );
      try (
          OutputStream infoFile = Files.newOutputStream(getInfoFile(name));
          OutputStream pipelineFile = Files.newOutputStream(getPipelineFile(name));
        ){
        pipeline.setUuid(uuid);
        json.writeValue(infoFile, BeanHelper.wrapPipelineInfo(info));
        json.writeValue(pipelineFile, BeanHelper.wrapPipelineConfiguration(pipeline));
        if (pipelineStateStore != null) {
          List<Issue> errors = new ArrayList<>();
          PipelineBeanCreator.get().create(pipeline, errors, null);
          pipelineStateStore.edited(user, name, tag,  PipelineBeanCreator.get().getExecutionMode(pipeline, errors), false);
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
      try (InputStream pipelineFile = Files.newInputStream(getPipelineFile(name))) {
        PipelineInfo info = getInfo(name);
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
              new ArrayList<MetricsRuleDefinition>(),
              new ArrayList<DataRuleDefinition>(),
              new ArrayList<DriftRuleDefinition>(),
              new ArrayList<String>(),
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
  public boolean deleteRules(String name) throws PipelineStoreException {
    synchronized (lockCache.getLock(name)) {
      pipelineToRuleDefinitionMap.remove(getPipelineKey(name, REV));

      if (hasPipeline(name) && getRulesFile(name) != null) {
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

  public String getPipelineKey(String pipelineName, String rev) {
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

      try (
          OutputStream infoFile = Files.newOutputStream(getInfoFile(name));
          OutputStream pipelineFile = Files.newOutputStream(getPipelineFile(name));
      ) {
        json.writeValue(infoFile, BeanHelper.wrapPipelineInfo(updatedInfo));
        json.writeValue(pipelineFile, BeanHelper.wrapPipelineConfiguration(savedPipeline));
      } catch (Exception ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0204, name, ex.toString(), ex);
      }
      return savedPipeline;
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
    Object isRemote = pipelineStateStore
        .getState(name, rev)
        .getAttributes()
        .get(RemoteDataCollector.IS_REMOTE_PIPELINE);
    // remote attribute will be null for pipelines with version earlier than 1.3
    return isRemote != null && (boolean) isRemote;
  }

}
