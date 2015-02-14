/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.MetricsRuleDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.RuleDefinitions;
import com.streamsets.pipeline.io.DataStore;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.store.PipelineInfo;
import com.streamsets.pipeline.store.PipelineRevInfo;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.AbstractTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class FilePipelineStoreTask extends AbstractTask implements PipelineStoreTask {

  private static final Logger LOG = LoggerFactory.getLogger(FilePipelineStoreTask.class);

  public static final String CREATE_DEFAULT_PIPELINE_KEY = "create.default.pipeline";
  public static final boolean CREATE_DEFAULT_PIPELINE_DEFAULT = true;
  private static final String STAGE_PREFIX = "stage.";
  private static final String DOT_REGEX = "\\.";

  @VisibleForTesting
  static final String DEFAULT_PIPELINE_NAME = "xyz";

  @VisibleForTesting
  static final String DEFAULT_PIPELINE_DESCRIPTION = "Default Pipeline";

  @VisibleForTesting
  static final String SYSTEM_USER = "system";

  @VisibleForTesting
  static final String REV = "0";

  private static final String INFO_FILE = "info.json";
  private static final String PIPELINE_FILE = "pipeline.json";
  private static final String RULES_FILE = "rules.json";

  private final RuntimeInfo runtimeInfo;
  private final Configuration conf;
  private File storeDir;
  private ObjectMapper json;

  //rules can be modified while the pipeline is running
  //runner will look up the rule definition before running each batch, we need synchronization
  private final Object rulesMutex;
  private HashMap<String, RuleDefinitions> pipelineToRuleDefinitionMap;

  @Inject
  public FilePipelineStoreTask(RuntimeInfo runtimeInfo, Configuration conf) {
    super("filePipelineStore");
    this.runtimeInfo = runtimeInfo;
    this.conf = conf;
    json = ObjectMapperFactory.get();
    rulesMutex = new Object();
    pipelineToRuleDefinitionMap = new HashMap<>();
  }

  @VisibleForTesting
  File getStoreDir() {
    return storeDir;
  }

  @Override
  protected void initTask()  {
    storeDir = new File(runtimeInfo.getDataDir(), "pipelines");
    if (!storeDir.exists()) {
      if (!storeDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create directory '{}'", storeDir.getAbsolutePath()));
      }
    }
    if (conf.get(CREATE_DEFAULT_PIPELINE_KEY, CREATE_DEFAULT_PIPELINE_DEFAULT)) {
      if (!hasPipeline(DEFAULT_PIPELINE_NAME)) {
        try {
          create(DEFAULT_PIPELINE_NAME, DEFAULT_PIPELINE_DESCRIPTION, SYSTEM_USER);
        } catch (PipelineStoreException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  }

  @Override
  protected void stopTask() {
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
  public PipelineConfiguration create(String name, String description, String user) throws PipelineStoreException {
    if (hasPipeline(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0201, name);
    }
    if (!getPipelineDir(name).mkdir()) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0202, name,
                                       Utils.format("'{}' mkdir failed", getPipelineDir(name)));
    }
    Date date = new Date();
    UUID uuid = UUID.randomUUID();
    PipelineInfo info = new PipelineInfo(name, description, date, date, user, user, REV, uuid, false);

    List<ConfigConfiguration> configuration = new ArrayList<>(2);
    configuration.add(new ConfigConfiguration("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    configuration.add(new ConfigConfiguration("badRecordsHandling", ""));

    PipelineConfiguration pipeline = new PipelineConfiguration(SCHEMA_VERSION, uuid, configuration, null,
      null, null);
    pipeline.setDescription(description);
    try {
      json.writeValue(getInfoFile(name), info);
      json.writeValue(getPipelineFile(name), pipeline);
    } catch (Exception ex) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0202, name, ex.getMessage(),
                                       ex);
    }
    pipeline.setPipelineInfo(info);
    return pipeline;
  }

  private boolean deleteAll(File path) {
    boolean ok = true;
    File[] children = path.listFiles();
    if (children != null) {
      for (File child : children) {
        ok = deleteAll(child);
        if (!ok) {
          break;
        }
      }
    }
    return ok && path.delete();
  }

  private boolean cleanUp(String name) throws PipelineStoreException {
    return deleteAll(getPipelineDir(name));
  }

  @Override
  public void delete(String name) throws PipelineStoreException {
    if (!hasPipeline(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
    }
    if (!cleanUp(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0203, name);
    }
    cleanUp(name);
  }

  private PipelineInfo getInfo(String name, boolean checkExistence) throws PipelineStoreException {
    if (checkExistence && !hasPipeline(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
    }
    try {
      return json.readValue(getInfoFile(name), PipelineInfo.class);
    } catch (Exception ex) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0206, name);
    }
  }

  @Override
  public List<PipelineInfo> getPipelines() throws PipelineStoreException {
    List<PipelineInfo> list = new ArrayList<>();
    for (String name : storeDir.list(new FilenameFilter() {
          @Override
          public boolean accept(File dir, String name) {
            //If one browses to the pipelines directory, mac creates a ".DS_store directory and this causes us problems
            //So filter it out
            if(name.startsWith(".")) {
              return false;
            }
            return true;
          }
        })) {
      list.add(getInfo(name, false));
    }
    return Collections.unmodifiableList(list);
  }

  @Override
  public PipelineInfo getInfo(String name) throws PipelineStoreException {
    return getInfo(name, true);
  }

  @Override
  public PipelineConfiguration save(String name, String user, String tag, String tagDescription,
      PipelineConfiguration pipeline) throws PipelineStoreException {
    if (!hasPipeline(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
    }
    PipelineInfo savedInfo = getInfo(name, false);
    if (!savedInfo.getUuid().equals(pipeline.getUuid())) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0205, name);
    }
    UUID uuid = UUID.randomUUID();
    PipelineInfo info = new PipelineInfo(getInfo(name, false), pipeline.getDescription(), new Date(), user, REV, uuid,
                                         pipeline.isValid());
    try {
      pipeline.setUuid(uuid);
      json.writeValue(getInfoFile(name), info);
      json.writeValue(getPipelineFile(name), pipeline);
    } catch (Exception ex) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0204, name, ex.getMessage(), ex);
    }
    pipeline.setPipelineInfo(info);
    return pipeline;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<PipelineRevInfo> getHistory(String name) throws PipelineStoreException {
    return ImmutableList.of(new PipelineRevInfo(getInfo(name, true)));
  }

  @Override
  public PipelineConfiguration load(String name, String tagOrRev) throws PipelineStoreException {
    if (!hasPipeline(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
    }
    try {
      PipelineInfo info = getInfo(name);
      PipelineConfiguration pipeline = json.readValue(getPipelineFile(name), PipelineConfiguration.class);
      pipeline.setPipelineInfo(info);
      return pipeline;
    } catch (Exception ex) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0206, name, ex.getMessage(),
                                       ex);
    }
  }

  @Override
  public RuleDefinitions retrieveRules(String name, String tagOrRev) throws PipelineStoreException {
    synchronized (rulesMutex) {
      if(!pipelineToRuleDefinitionMap.containsKey(getPipelineKey(name, tagOrRev))) {
        if (!hasPipeline(name)) {
          throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
        }
        //try loading from store, needed in cases like restart
        RuleDefinitions ruleDefinitions = null;
        File rulesFile = getRulesFile(name);
        if(rulesFile.exists()) {
          try {
            ruleDefinitions = ObjectMapperFactory.get().readValue(
              new DataStore(rulesFile).getInputStream(), RuleDefinitions.class);
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
    if (!hasPipeline(pipelineName)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, pipelineName);
    }
    synchronized (rulesMutex) {
      //check the uuid of the existing rule definition to detect any change since the previous load.
      //two browsers could modify the same rule definition
      if(pipelineToRuleDefinitionMap.get(getPipelineKey(pipelineName, tag)) != null) {
        UUID savedUuid = pipelineToRuleDefinitionMap.get(getPipelineKey(pipelineName, tag)).getUuid();
        if (!savedUuid.equals(ruleDefinitions.getUuid())) {
          throw new PipelineStoreException(ContainerError.CONTAINER_0205, pipelineName);
        }
      }

      UUID uuid = UUID.randomUUID();
      ruleDefinitions.setUuid(uuid);
      try {
        ObjectMapperFactory.get().writeValue(new DataStore(getRulesFile(pipelineName)).getOutputStream(),
          ruleDefinitions);
        pipelineToRuleDefinitionMap.put(getPipelineKey(pipelineName, tag), ruleDefinitions);
      } catch (IOException ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0404, pipelineName, ex.getMessage(),
          ex);
      }
    }
    return ruleDefinitions;
  }

  @Override
  public boolean deleteRules(String name) throws PipelineStoreException {
    if(hasPipeline(name) && getRulesFile(name) != null && getRulesFile(name).exists()) {
      return getRulesFile(name).delete();
    }
    return false;
  }
  public String getPipelineKey(String pipelineName, String rev) {
    return pipelineName + "$" + rev;
  }

}
