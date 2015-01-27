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
import com.streamsets.pipeline.config.AlertDefinition;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.MetricDefinition;
import com.streamsets.pipeline.config.MetricsAlertDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.config.SamplingDefinition;
import com.streamsets.pipeline.config.StageConfiguration;
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
import com.streamsets.pipeline.validation.RuleIssue;
import com.streamsets.pipeline.validation.ValidationError;

import javax.inject.Inject;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class FilePipelineStoreTask extends AbstractTask implements PipelineStoreTask {
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

  @Inject
  public FilePipelineStoreTask(RuntimeInfo runtimeInfo, Configuration conf) {
    super("filePipelineStore");
    this.runtimeInfo = runtimeInfo;
    this.conf = conf;
    json = ObjectMapperFactory.get();
    rulesMutex = new Object();
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

  private File getPipelineDir(String name) {
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
    configuration.add(new ConfigConfiguration("stopPipelineOnError", false));
    PipelineConfiguration pipeline = new PipelineConfiguration(uuid, configuration, null,
      null);
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
  public RuleDefinition retrieveRules(String name, String tagOrRev) throws PipelineStoreException {
    if (!hasPipeline(name)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
    }
    if(!getRulesFile(name).exists()) {
      //Has no rule definition, return empty rules
      return new RuleDefinition(Collections.<com.streamsets.pipeline.config.AlertDefinition>emptyList(),
        Collections.<com.streamsets.pipeline.config.MetricsAlertDefinition>emptyList(),
        Collections.<com.streamsets.pipeline.config.SamplingDefinition>emptyList(),
        Collections.<com.streamsets.pipeline.config.MetricDefinition>emptyList());
    }
    synchronized (rulesMutex) {
      try {
        RuleDefinition ruleDefinition = ObjectMapperFactory.get().readValue(
          new DataStore(getRulesFile(name)).getInputStream(), RuleDefinition.class);
        return ruleDefinition;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public RuleDefinition storeRules(String pipelineName, String tag, RuleDefinition ruleDefinition)
    throws PipelineStoreException {
    if (!hasPipeline(pipelineName)) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0200, pipelineName);
    }
    RuleDefinition validatedRuleDefinition = validateRuleDefinition(pipelineName, tag, ruleDefinition);
    synchronized (rulesMutex) {
      try {
        ObjectMapperFactory.get().writeValue(new DataStore(getRulesFile(pipelineName)).getOutputStream(),
          validatedRuleDefinition);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return validatedRuleDefinition;
    }
  }

  @Override
  public boolean deleteRules(String name) throws PipelineStoreException {
    if(hasPipeline(name) && getRulesFile(name) != null && getRulesFile(name).exists()) {
      return getRulesFile(name).delete();
    }
    return false;
  }

  private RuleDefinition validateRuleDefinition(String name, String rev, RuleDefinition ruleDefinition)
    throws PipelineStoreException {
    PipelineConfiguration pipelineConfig = load(name, rev);
    Set<String> pipelineOutputLanes = new HashSet<>();
    Set<String> stageInstanceNames = new HashSet<>();

    for(StageConfiguration stageConfiguration : pipelineConfig.getStages()) {
      pipelineOutputLanes.addAll(stageConfiguration.getOutputLanes());
      stageInstanceNames.add(stageConfiguration.getInstanceName());
    }

    //TODO: Also validate all the EL expressions and create issues if they are not valid.
    //TODO: Create a RuleDefinition validator similar to PipelineConfigurationValidator

    List<RuleIssue> ruleIssues = new ArrayList<>();
    for(AlertDefinition alertDefinition : ruleDefinition.getAlertDefinitions()) {
      if(!pipelineOutputLanes.contains(alertDefinition.getLane())) {
        ruleIssues.add(RuleIssue.createRuleIssue(alertDefinition.getId(), ValidationError.VALIDATION_0027,
          alertDefinition.getId(), alertDefinition.getLane()));
      }
    }
    for(MetricDefinition metricDefinition : ruleDefinition.getMetricDefinitions()) {
      if(!pipelineOutputLanes.contains(metricDefinition.getLane())) {
        ruleIssues.add(RuleIssue.createRuleIssue(metricDefinition.getId(), ValidationError.VALIDATION_0027,
          metricDefinition.getId(), metricDefinition.getLane()));
      }
    }
    for(SamplingDefinition samplingDefinition : ruleDefinition.getSamplingDefinitions()) {
      if(!pipelineOutputLanes.contains(samplingDefinition.getLane())) {
        ruleIssues.add(RuleIssue.createRuleIssue(samplingDefinition.getId(), ValidationError.VALIDATION_0027,
          samplingDefinition.getId(), samplingDefinition.getLane()));
      }
    }
    for(MetricsAlertDefinition metricsAlertDefinition : ruleDefinition.getMetricsAlertDefinitions()) {
      if(metricsAlertDefinition.getMetricId() != null &&
        metricsAlertDefinition.getMetricId().startsWith(STAGE_PREFIX)) {
        String[] strings = metricsAlertDefinition.getMetricId().split(DOT_REGEX);
        String instanceName = strings[1];
        if (!stageInstanceNames.contains(instanceName)) {
          ruleIssues.add(RuleIssue.createRuleIssue(metricsAlertDefinition.getId(), ValidationError.VALIDATION_0028,
            metricsAlertDefinition.getId(), instanceName));
        }
      }
    }
    ruleDefinition.setIssues(ruleIssues);
    return ruleDefinition;
  }

}
