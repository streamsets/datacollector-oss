/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.streamsets.dc.execution.PipelineStateStore;
import com.streamsets.dc.execution.PipelineStatus;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.config.MetricsRuleDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.PipelineDefinition;
import com.streamsets.pipeline.config.RuleDefinitions;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.creation.PipelineBeanCreator;
import com.streamsets.pipeline.creation.PipelineConfigBean;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


public class FilePipelineStoreTask extends AbstractTask implements PipelineStoreTask {

  private static final Logger LOG = LoggerFactory.getLogger(FilePipelineStoreTask.class);

  @VisibleForTesting
  static final String REV = "0";

  private static final String INFO_FILE = "info.json";
  private static final String PIPELINE_FILE = "pipeline.json";
  private static final String RULES_FILE = "rules.json";

  private final StageLibraryTask stageLibrary;
  private final RuntimeInfo runtimeInfo;
  private File storeDir;
  private final ObjectMapper json;
  private final PipelineStateStore pipelineStateStore;

  //rules can be modified while the pipeline is running
  //runner will look up the rule definition before running each batch, we need synchronization
  private final Object rulesMutex;
  private final HashMap<String, RuleDefinitions> pipelineToRuleDefinitionMap;

  @Inject
  public FilePipelineStoreTask(RuntimeInfo runtimeInfo, StageLibraryTask stageLibrary, PipelineStateStore pipelineStateStore) {
    super("filePipelineStore");
    this.stageLibrary = stageLibrary;
    this.runtimeInfo = runtimeInfo;
    json = ObjectMapperFactory.get();
    rulesMutex = new Object();
    pipelineToRuleDefinitionMap = new HashMap<>();
    this.pipelineStateStore = pipelineStateStore;
  }

  @VisibleForTesting
  File getStoreDir() {
    return storeDir;
  }

  @Override
  public void initTask() {
    if (runtimeInfo.getExecutionMode() == RuntimeInfo.ExecutionMode.SLAVE) {
      storeDir = new File(runtimeInfo.getDataDir());
    } else {
      storeDir = new File(runtimeInfo.getDataDir(), "pipelines");
    }
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
    PipelineConfiguration pipeline = new PipelineConfiguration(SCHEMA_VERSION, uuid, description,
                                                               stageLibrary.getPipeline().getPipelineDefaultConfigs(),
                                                               Collections.EMPTY_MAP, Collections.EMPTY_LIST, null);
    try {
      json.writeValue(getInfoFile(name), BeanHelper.wrapPipelineInfo(info));
      json.writeValue(getPipelineFile(name), BeanHelper.wrapPipelineConfiguration(pipeline));
    } catch (Exception ex) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0202, name, ex.getMessage(),
                                       ex);
    }
    pipeline.setPipelineInfo(info);
    if (pipelineStateStore != null) {
      pipelineStateStore.saveState(user, name, REV, PipelineStatus.EDITED, "Pipeline edited", null,
                                   ExecutionMode.STANDALONE);
    }
    return pipeline;
  }

  private boolean cleanUp(String name) throws PipelineStoreException {
    return PipelineDirectoryUtil.deleteAll(getPipelineDir(name));
  }

  @Override
  public synchronized void delete(String name) throws PipelineStoreException {
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
    cleanUp(name);
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

  @Override
  public PipelineConfiguration save(String user, String name, String tag, String tagDescription,
      PipelineConfiguration pipeline) throws PipelineStoreException {
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
    PipelineInfo info = new PipelineInfo(getInfo(name), pipeline.getDescription(), new Date(), user, REV, uuid,
                                         pipeline.isValid());

    //synchronize pipeline configuration with whats present in the stage library
    syncPipelineConfiguration(pipeline, name, tag, stageLibrary);

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

  @Override
  @SuppressWarnings("unchecked")
  public List<PipelineRevInfo> getHistory(String name) throws PipelineStoreException {
    return ImmutableList.of(new PipelineRevInfo(getInfo(name)));
  }

  @Override
  public PipelineConfiguration load(String name, String tagOrRev) throws PipelineStoreException {
    if (!hasPipeline(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
    }
    try {
      PipelineInfo info = getInfo(name);
      PipelineConfigurationJson pipelineConfigBean = json.readValue(getPipelineFile(name),
        PipelineConfigurationJson.class);
      PipelineConfiguration pipeline = pipelineConfigBean.getPipelineConfiguration();
      pipeline.setPipelineInfo(info);
      syncPipelineConfiguration(pipeline, name, tagOrRev, stageLibrary);
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
      try (OutputStream os = new DataStore(getRulesFile(pipelineName)).getOutputStream()){
        ObjectMapperFactory.get().writeValue(os, BeanHelper.wrapRuleDefinitions(ruleDefinitions));
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
    synchronized (rulesMutex) {
      pipelineToRuleDefinitionMap.remove(getPipelineKey(name, REV));
    }
    if(hasPipeline(name) && getRulesFile(name) != null && getRulesFile(name).exists()) {
      return getRulesFile(name).delete();
    }
    return false;
  }

  public String getPipelineKey(String pipelineName, String rev) {
    return pipelineName + "$" + rev;
  }

  @VisibleForTesting
  /**
   * Compares the argument PipelineConfiguration object with the pipeline definition available in the stage library.
   *
   * Expected but missing configuration will be added with default values and unexpected but existing configurations
   * will be removed.
   *
   * Pipeline Error Stage configuration, stage configuration and pipeline configuration options are checked.
   *
   * @param pipelineConfig
   * @param name
   * @param tagOrRev
   * @param stageLibrary
   * @throws PipelineStoreException if the stage definition is found in the the library.
   */
  PipelineConfiguration syncPipelineConfiguration(PipelineConfiguration pipelineConfig, String name, String tagOrRev,
                                         StageLibraryTask stageLibrary)
    throws PipelineStoreException {

    //sync error stage configuration options
    StageConfiguration errorStageConf = syncStageConfiguration(pipelineConfig.getErrorStage(), stageLibrary);

    //sync pipeline configuration options
    List<ConfigConfiguration> pipelineConfigs = syncPipelineConf(pipelineConfig, name, tagOrRev);

    //sync stage configurations
    List<StageConfiguration> stageConfigs = new ArrayList<>();
    for(StageConfiguration argStageConf : pipelineConfig.getStages()) {
      stageConfigs.add(syncStageConfiguration(argStageConf, stageLibrary));
    }

    return new PipelineConfiguration(pipelineConfig.getSchemaVersion(), pipelineConfig.getUuid(),
      pipelineConfig.getDescription(), pipelineConfigs, pipelineConfig.getUiInfo(), stageConfigs, errorStageConf);
  }

  private List<ConfigConfiguration> syncPipelineConf(PipelineConfiguration pipelineConfig, String name,
                                                     String tagOrRev) {
    PipelineDefinition pipelineDef = PipelineDefinition.getPipelineDef();
    Set<String> configsToRemove = getNamesFromConfigConf(pipelineConfig.getConfiguration());

    List<ConfigConfiguration> configuration = new ArrayList<>(pipelineConfig.getConfiguration());

    for(ConfigDefinition configDefinition : pipelineDef.getConfigDefinitions()) {
      ConfigConfiguration configConf = pipelineConfig.getConfiguration(configDefinition.getName());
      if(configConf == null) {
        LOG.warn("Pipeline [name : {}, version : {}] does not have expected configuration '{}'. " +
            "Adding the configuration with default value '{}'", name, tagOrRev, configDefinition.getName(),
          configDefinition.getDefaultValue());
        configConf = new ConfigConfiguration(configDefinition.getName(), configDefinition.getDefaultValue());
        configuration.add(configConf);
      }
      configsToRemove.remove(configDefinition.getName());
      //No complex configurations are expected in the pipeline configurations as of now.
    }

    if(configsToRemove.size() > 0) {
      List<ConfigConfiguration> remove = new ArrayList<>();
      for(ConfigConfiguration c : pipelineConfig.getConfiguration()) {
        if(configsToRemove.contains(c.getName())) {
          remove.add(c);
        }
      }
      configuration.removeAll(remove);
    }
    return configuration;
  }

  private StageConfiguration syncStageConfiguration(StageConfiguration argStageConf, StageLibraryTask stageLibrary)
    throws PipelineStoreException {
    if(argStageConf == null) {
      return null;
    }
    //get the definition of this stage from the stage library. This is the source of truth.
    //The configuration object must adhere to this definition.
    StageDefinition stageDef = stageLibrary.getStage(argStageConf.getLibrary(), argStageConf.getStageName(),
      argStageConf.getStageVersion());

    if(stageDef == null) {
      //Encountered a stage whose definition is not available - can happen if the pipeline was designed in an
      //environment where there is a stage library X and then the pipeline is exported and imported in to this
      //setup where there is no library X
      throw new PipelineStoreException(ContainerError.CONTAINER_0207, argStageConf.getStageName(),
        argStageConf.getLibrary(), argStageConf.getStageVersion());
    }

    //Collect all the available configuration names from the stage configuration
    Set<String> configsToRemove = getNamesFromConfigConf(argStageConf.getConfiguration());

    //Updated list of configurations
    Map<String, ConfigConfiguration> configuration = new LinkedHashMap<>();
    for(ConfigConfiguration c : argStageConf.getConfiguration()) {
      configuration.put(c.getName(), c);
    }

    //go over every config def and make sure it is present in the config configuration
    for(ConfigDefinition configDef : stageDef.getConfigDefinitions()) {
      ConfigConfiguration configConf = argStageConf.getConfig(configDef.getName());
      if(configConf == null) {
        LOG.warn("Stage [name : {}, library : {}, version : {}] does not have expected configuration '{}'. " +
            "Adding the configuration with default value '{}'",
          argStageConf.getStageName(), argStageConf.getLibrary(), argStageConf.getStageVersion(), configDef.getName(),
          configDef.getDefaultValue());
        configConf = new ConfigConfiguration(configDef.getName(), configDef.getDefaultValue());
        configuration.put(configDef.getName(), configConf);
      }
      if(configDef.getModel() != null && configDef.getModel().getConfigDefinitions() != null
        && !configDef.getModel().getConfigDefinitions().isEmpty()) {
        //complex field
        //ConfigConfiguration for this will be of the form List - > HashMap <String, Object>
        // where each HashMap contains entries of the form "nested config name"  - > value
        List<Map<String, Object>> value = (List<Map<String, Object>>)configConf.getValue();
        if(value == null) {
          value = new ArrayList<>();
          configConf = new ConfigConfiguration(configConf.getName(), value);
          configuration.put(configConf.getName(), configConf);
        }
        if(value.isEmpty()) {
          value.add(new HashMap<String, Object>());
        }
        for(ConfigDefinition c : configDef.getModel().getConfigDefinitions()) {
          for(Map<String, Object> map : value) {
            if(!map.containsKey(c.getName())) {
              LOG.warn("Stage [name : {}, library : {}, version : {}] does not have expected configuration '{}'. " +
                  "Adding the configuration with default value '{}'",
                argStageConf.getStageName(), argStageConf.getLibrary(), argStageConf.getStageVersion(),
                configDef.getName() + "/" + c.getName(), c.getDefaultValue());
              map.put(c.getName(), c.getDefaultValue());
            }
            configsToRemove.remove(configDef.getName() + "/" + c.getName());
          }
        }
      }
      configsToRemove.remove(configDef.getName());
    }

    //Remove unexpected configurations
    if(configsToRemove.size() > 0) {
      List<ConfigConfiguration> remove = new ArrayList<>();
      for (ConfigConfiguration c : configuration.values()) {
        if (configsToRemove.contains(c.getName())) {
          remove.add(c);
        }
        if (c.getValue() != null && c.getValue() instanceof List) {
          for (Object object : (List) c.getValue()) {
            if (object instanceof Map) {
              Map<String, Object> map = (Map) object;
              if(isComplexConfiguration(map)) {
                List<String> keysToRemove = new ArrayList<>();
                for (String key : map.keySet()) {
                  if (configsToRemove.contains(c.getName() + "/" + key)) {
                    keysToRemove.add(key);
                  }
                }
                for (String key : keysToRemove) {
                  map.remove(key);
                }
              }
            }
          }
        }
      }
      for(ConfigConfiguration c : remove) {
        configuration.remove(c.getName());
      }
    }

    List<ConfigConfiguration> config = new ArrayList<>();
    for(ConfigConfiguration c : configuration.values()) {
      config.add(c);
    }
    return new StageConfiguration(argStageConf.getInstanceName(), argStageConf.getLibrary(),
      argStageConf.getStageName(), argStageConf.getStageVersion(), config , argStageConf.getUiInfo(),
      argStageConf.getInputLanes(), argStageConf.getOutputLanes());
  }

  /**
   * Retrieves names of all the configurations from the given ConfigConfiguration objects.
   * Includes nested configuration objects for complex configurations.
   *
   * Complex configuration names will be of the form <parentConfigName>/<childConfigName>
   *
   * @param configDefs
   * @return
   */
  private Set<String> getNamesFromConfigConf(List<ConfigConfiguration> configDefs) {
    Set<String> expectedConfigNames = new HashSet<>();
    for(ConfigConfiguration c : configDefs) {
      expectedConfigNames.add(c.getName());
      if(c.getValue() instanceof List) {
        for (Object object : (List) c.getValue()) {
          if (object instanceof Map) {
            Map<String, Object> map = (Map) object;
            if(isComplexConfiguration(map)) {
              for (String key : map.keySet()) {
                expectedConfigNames.add(c.getName() + "/" + key);
              }
            }
          }
        }
      }
    }
    return expectedConfigNames;
  }

  private boolean isComplexConfiguration(Map<String, Object> map) {
    if(!map.isEmpty()
      && map.keySet().size() == 2 //regular configuration contains 2 entries
      && ((map.keySet().contains("key") && map.keySet().contains("value")) //one of them with key "key" & "value"
      || (map.keySet().contains("outputLane") && map.keySet().contains("predicate")))) { //lanePredicate case
      return false;
    }
    return true;
  }

}
