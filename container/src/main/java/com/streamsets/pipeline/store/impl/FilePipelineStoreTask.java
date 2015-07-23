/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.config.MetricsRuleDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.RuleDefinitions;
import com.streamsets.pipeline.creation.PipelineBeanCreator;
import com.streamsets.pipeline.creation.PipelineConfigBean;
import com.streamsets.dc.execution.PipelineStateStore;
import com.streamsets.dc.execution.PipelineStatus;
import com.streamsets.pipeline.io.DataStore;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.restapi.bean.PipelineConfigurationJson;
import com.streamsets.pipeline.restapi.bean.PipelineInfoJson;
import com.streamsets.pipeline.restapi.bean.RuleDefinitionsJson;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineInfo;
import com.streamsets.pipeline.store.PipelineRevInfo;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.AbstractTask;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.LockCache;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;
import com.streamsets.pipeline.validation.Issue;

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
import java.util.List;
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
  private static final String RULES_FILE = "rules.json";

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
    return new File(storeDir, PipelineDirectoryUtil.getEscapedPipelineName(name));
  }

  @VisibleForTesting
  File getInfoFile(String name) {
    return new File(getPipelineDir(name), INFO_FILE);
  }

  private File getPipelineFile(String name) {
    return new File(getPipelineDir(name), PIPELINE_FILE);
  }

  private File getRulesFile(String name) {
    return new File(getPipelineDir(name), RULES_FILE);
  }

  @Override
  public boolean hasPipeline(String name) {
    return getPipelineDir(name).exists();
  }

  @Override
  public PipelineConfiguration create(String user, String name, String description) throws PipelineStoreException {
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
          .getPipeline().getPipelineDefaultConfigs(), Collections.EMPTY_MAP, Collections.EMPTY_LIST, null);

      try {
        json.writeValue(getInfoFile(name), BeanHelper.wrapPipelineInfo(info));
        json.writeValue(getPipelineFile(name), BeanHelper.wrapPipelineConfiguration(pipeline));
      } catch (Exception ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0202, name, ex.getMessage(), ex);
      }
      pipeline.setPipelineInfo(info);
      if (pipelineStateStore != null) {
        pipelineStateStore.saveState(user, name, REV, PipelineStatus.EDITED, "Pipeline edited", null,
          ExecutionMode.STANDALONE);
      }
      return pipeline;
    }
  }

  private boolean cleanUp(String name) throws PipelineStoreException {
    return PipelineDirectoryUtil.deleteAll(getPipelineDir(name));
  }

  @Override
  public void delete(String name) throws PipelineStoreException {
    synchronized (lockCache.getLock(name)) {
      if (!hasPipeline(name)) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
      }
      if (!cleanUp(name)) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0203, name);
      }
      if (pipelineStateStore != null) {
        // For now, passing rev 0 - make delete take tag/rev as a parameter
        PipelineStatus pipelineStatus = pipelineStateStore.getState(name, REV).getStatus();
        if (pipelineStatus.isActive()) {
          throw new PipelineStoreException(ContainerError.CONTAINER_0208, pipelineStatus);
        }
        pipelineStateStore.delete(name, REV);
      }
    }
  }

  @Override
  public List<PipelineInfo> getPipelines() throws PipelineStoreException {
    List<PipelineInfo> pipelineInfoList = new ArrayList<PipelineInfo>();
    for (String name : storeDir.list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        // If one browses to the pipelines directory, mac creates a ".DS_store directory and this causes us problems
        // So filter it out
        if (name.startsWith(".")) {
          return false;
        }
        return true;
      }

    })) {
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
          PipelineConfigBean pipelineConfigBean = PipelineBeanCreator.get().create(pipeline, errors);
          if (pipelineConfigBean == null) {
            throw new PipelineStoreException(ContainerError.CONTAINER_0116, errors);
          }
          pipelineStateStore.edited(user, name, tag, pipelineConfigBean.executionMode);
        }

      } catch (Exception ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0204, name, ex.getMessage(), ex);
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
        return pipeline;
      }
      catch (Exception ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0206, name, ex.getMessage(), ex);
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
            LOG.debug(ContainerError.CONTAINER_0403.getMessage(), name, ex.getMessage(),
              ex);
            ruleDefinitions = null;
          }
        }
        if(ruleDefinitions == null) {
          ruleDefinitions = new RuleDefinitions(new ArrayList<MetricsRuleDefinition>(),
            new ArrayList<DataRuleDefinition>(), new ArrayList<String>(), UUID.randomUUID());
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
      try (OutputStream os = new DataStore(getRulesFile(pipelineName)).getOutputStream()) {
        ObjectMapperFactory.get().writeValue(os, BeanHelper.wrapRuleDefinitions(ruleDefinitions));
        pipelineToRuleDefinitionMap.put(getPipelineKey(pipelineName, tag), ruleDefinitions);
      } catch (IOException ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0404, pipelineName, ex.getMessage(), ex);
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

}
