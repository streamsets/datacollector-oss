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
package com.streamsets.datacollector.configupgrade;

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.IssueCreator;
import com.streamsets.datacollector.validation.ValidationError;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PipelineConfigurationUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigurationUpgrader.class);

  private static final PipelineConfigurationUpgrader UPGRADER = new PipelineConfigurationUpgrader() {
  };

  public static PipelineConfigurationUpgrader get() {
    return UPGRADER;
  }

  protected PipelineConfigurationUpgrader() {
  }

  public PipelineConfiguration upgradeIfNecessary(StageLibraryTask library, PipelineConfiguration pipelineConf, List<Issue> issues) {
    Preconditions.checkArgument(issues.size() == 0, "Given list of issues must be empty.");
    boolean upgrade;

    // Firstly upgrading schema if needed, then data
    upgrade = needsSchemaUpgrade(pipelineConf, issues);
    if(upgrade && issues.isEmpty()) {
      pipelineConf = upgradeSchema(pipelineConf, issues);
    }

    // Something went wrong with the schema upgrade
    if(!issues.isEmpty()) {
      return null;
    }

    // Upgrading data if needed
    upgrade = needsUpgrade(library, pipelineConf, issues);
    if (upgrade && issues.isEmpty()) {
      //we try to upgrade only if we have all defs for the pipelineConf
      pipelineConf = upgrade(library, pipelineConf, issues);
    }
    return (issues.isEmpty()) ? pipelineConf : null;
  }

  private boolean needsSchemaUpgrade(PipelineConfiguration pipelineConf, List<Issue> ownIssues) {
    return pipelineConf.getSchemaVersion() != PipelineStoreTask.SCHEMA_VERSION;
  }

  private PipelineConfiguration upgradeSchema(PipelineConfiguration pipelineConf, List<Issue> issues) {
    LOG.debug("Upgrading schema from version {} on pipeline {}", pipelineConf.getSchemaVersion(), pipelineConf.getUuid());
    switch (pipelineConf.getSchemaVersion()) {
      case 1:
        upgradeSchema1to2(pipelineConf, issues);
        // fall through
      case 2:
        upgradeSchema2to3(pipelineConf, issues);
        // fall through
      case 3:
        upgradeSchema3to4(pipelineConf, issues);
        // fall through
      case 4:
        upgradeSchema4to5(pipelineConf, issues);
        break;
      default:
        issues.add(IssueCreator.getPipeline().create(ValidationError.VALIDATION_0000, pipelineConf.getSchemaVersion()));
    }

    pipelineConf.setSchemaVersion(PipelineStoreTask.SCHEMA_VERSION);
    return issues.isEmpty() ? pipelineConf : null;
  }

  private void upgradeSchema1to2(PipelineConfiguration pipelineConf, List<Issue> issues) {
    // Version 1 did not have any eventLanes configuration so this will be null and we need to convert it
    // to empty list instead for all stages.

    // Normal stages
    for(StageConfiguration stage: pipelineConf.getStages()) {
      convertEventLaneNullToEmptyList(stage);
    }

    // Extra stages
    convertEventLaneNullToEmptyList(pipelineConf.getErrorStage());
    convertEventLaneNullToEmptyList(pipelineConf.getStatsAggregatorStage());
  }

  private void upgradeSchema2to3(PipelineConfiguration pipelineConf, List<Issue> issues) {
    // Added new attribute "pipelineId"
    if (pipelineConf.getPipelineId() == null) {
      pipelineConf.setPipelineId(pipelineConf.getInfo().getPipelineId());
    }
  }

  private void upgradeSchema3to4(PipelineConfiguration pipelineConf, List<Issue> issues) {
    // Added new attributes:
    // * statEventStages
    // * stopEventStages
    pipelineConf.setStartEventStages(Collections.emptyList());
    pipelineConf.setStopEventStages(Collections.emptyList());
  }

  private void upgradeSchema4to5(PipelineConfiguration pipelineConf, List<Issue> issues) {
    // Each stage have now a new field called "services" that we will by default fill with empty array

    // Normal stages
    for(StageConfiguration stage: pipelineConf.getStages()) {
      convertNullServiceToEmptyList(stage);
    }

    // Start event stages
    for(StageConfiguration stage : pipelineConf.getStartEventStages()) {
      convertNullServiceToEmptyList(stage);
    }

    // Stop event stages
    for(StageConfiguration stage : pipelineConf.getStopEventStages()) {
      convertNullServiceToEmptyList(stage);
    }
    // Extra stages
    convertNullServiceToEmptyList(pipelineConf.getErrorStage());
    convertNullServiceToEmptyList(pipelineConf.getStatsAggregatorStage());
  }

   private void convertNullServiceToEmptyList(StageConfiguration stage) {
    if(stage != null && stage.getServices() == null) {
      stage.setServices(Collections.emptyList());
    }
  }

  private void convertEventLaneNullToEmptyList(StageConfiguration stage) {
    if(stage != null && stage.getEventLanes() == null) {
      stage.setEventLanes(Collections.<String>emptyList());
    }
  }

  public StageDefinition getPipelineDefinition() {
    return PipelineBeanCreator.PIPELINE_DEFINITION;
  }

  boolean needsUpgrade(StageLibraryTask library, PipelineConfiguration pipelineConf, List<Issue> issues) {
    boolean upgrade;

    // pipeline confs
    StageConfiguration pipelineConfs = PipelineBeanCreator.getPipelineConfAsStageConf(pipelineConf);
    upgrade = needsUpgrade(getPipelineDefinition(), pipelineConfs, issues);

    // pipeline aggregating sink stage confs
    StageConfiguration statsAggTargetConf = pipelineConf.getStatsAggregatorStage();
    if (statsAggTargetConf != null) {
      StageDefinition def = library.getStage(statsAggTargetConf.getLibrary(), statsAggTargetConf.getStageName(), false);
      upgrade |= needsUpgrade(def, statsAggTargetConf, issues);
    }

    // pipeline error stage confs
    StageConfiguration errorStageConf = pipelineConf.getErrorStage();
    if (errorStageConf != null) {
      StageDefinition def = library.getStage(errorStageConf.getLibrary(), errorStageConf.getStageName(), false);
      upgrade |= needsUpgrade(def, errorStageConf, issues);
    }

    // pipeline stages confs
    for (StageConfiguration conf : pipelineConf.getStages()) {
      StageDefinition def = library.getStage(conf.getLibrary(), conf.getStageName(), false);
      upgrade |= needsUpgrade(def, conf, issues);
    }
    return upgrade;
  }

  static boolean needsUpgrade(StageDefinition def, StageConfiguration conf, List<Issue> issues) {
    boolean upgrade = false;
    if (def == null) {
      issues.add(IssueCreator.getStage(conf.getInstanceName()).create(ContainerError.CONTAINER_0901, conf.getLibrary(),
                                                                      conf.getStageName()));
    } else {
      int versionDiff = def.getVersion() - conf.getStageVersion();
      versionDiff = (versionDiff == 0) ? 0 : (versionDiff > 0) ? 1 : -1;
      switch (versionDiff) {
        case 0: // no change
          break;
        case 1: // current def is newer
          upgrade = true;
          break;
        case -1: // current def is older
          issues.add(IssueCreator.getStage(conf.getInstanceName()).create(ContainerError.CONTAINER_0902,
              conf.getLibrary(), conf.getStageName(), def.getVersion(), conf.getStageVersion(), conf.getInstanceName()));
          break;
        default:
          throw new IllegalStateException("Unexpected version diff " + versionDiff);
      }
    }
    return upgrade;
  }

  PipelineConfiguration upgrade(StageLibraryTask library, PipelineConfiguration pipelineConf, List<Issue> issues) {
    List<Issue> ownIssues = new ArrayList<>();

    // upgrade pipeline level configs if necessary
    StageConfiguration pipelineConfs = PipelineBeanCreator.getPipelineConfAsStageConf(pipelineConf);
    if (needsUpgrade(getPipelineDefinition(), pipelineConfs, issues)) {
      String sourceName = null;
      for (StageConfiguration stageConf: pipelineConf.getStages()) {
        if (stageConf.getInputLanes().isEmpty()) {
          sourceName = stageConf.getStageName();
        }
      }
      List<Config> configList = pipelineConfs.getConfiguration();
      // config 'sourceName' used by upgrader v3 to v4
      configList.add(new Config("sourceName", sourceName));
      pipelineConfs.setConfig(configList);
      pipelineConfs = upgrade(getPipelineDefinition(), pipelineConfs, issues);
      configList = pipelineConfs.getConfiguration();
      int index = -1;
      for (int i = 0; i < configList.size(); i++) {
        Config config = configList.get(i);
        if (config.getName().equals("sourceName")) {
          index = i;
          break;
        }
      }
      configList.remove(index);
      pipelineConfs.setConfig(configList);
    }
    List<StageConfiguration> stageConfs = new ArrayList<>();


    // upgrade aggregating stage if present and if necessary
    StageConfiguration statsAggregatorStageConf = pipelineConf.getStatsAggregatorStage();
    if (statsAggregatorStageConf != null) {
      StageDefinition def = library.getStage(statsAggregatorStageConf.getLibrary(), statsAggregatorStageConf.getStageName(), false);
      if (needsUpgrade(def, statsAggregatorStageConf, ownIssues)) {
        statsAggregatorStageConf = upgrade(def, statsAggregatorStageConf, ownIssues);
      }
    }

    // upgrade error stage if present and if necessary
    StageConfiguration errorStageConf = pipelineConf.getErrorStage();
    if (errorStageConf != null) {
      StageDefinition def = library.getStage(errorStageConf.getLibrary(), errorStageConf.getStageName(), false);
      if (needsUpgrade(def, errorStageConf, ownIssues)) {
        errorStageConf = upgrade(def, errorStageConf, ownIssues);
      }
    }

    // upgrade stages;
    for (StageConfiguration stageConf : pipelineConf.getStages()) {
      StageDefinition def = library.getStage(stageConf.getLibrary(), stageConf.getStageName(), false);
      if (needsUpgrade(def, stageConf, ownIssues)) {
        stageConf = upgrade(def, stageConf, ownIssues);
      }
      if (stageConf != null) {
        stageConfs.add(stageConf);
      }
    }

    // if ownIssues > 0 we had an issue upgrading, we wont touch the pipelineConf and return null
    if (ownIssues.isEmpty()) {
      pipelineConf.setConfiguration(pipelineConfs.getConfiguration());
      pipelineConf.setVersion(pipelineConfs.getStageVersion());
      pipelineConf.setErrorStage(errorStageConf);
      pipelineConf.setStatsAggregatorStage(statsAggregatorStageConf);
      pipelineConf.setStages(stageConfs);
    } else {
      issues.addAll(ownIssues);
      pipelineConf = null;
    }

    return pipelineConf;
  }

  static StageConfiguration upgrade(StageDefinition def, StageConfiguration conf, List<Issue> issues) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    int fromVersion = conf.getStageVersion();
    int toVersion = def.getVersion();
    try {
      Thread.currentThread().setContextClassLoader(def.getStageClassLoader());
      LOG.warn("Upgraded instance '{}' from version '{}' to version '{}'", conf.getInstanceName(), fromVersion,
               toVersion);
      List<Config> configs = def.getUpgrader().upgrade(def.getLibrary(), def.getName(), conf.getInstanceName(),
                                                       fromVersion, toVersion, conf.getConfiguration());
      conf.setStageVersion(def.getVersion());
      conf.setConfig(configs);
    } catch (StageException ex) {
      issues.add(IssueCreator.getStage(conf.getInstanceName()).create(ex.getErrorCode(), ex.getParams()));
    } catch (Exception ex) {
      LOG.error("Unknown exception during upgrade: " + ex, ex);
      issues.add(IssueCreator.getStage(conf.getInstanceName()).create(ContainerError.CONTAINER_0900, fromVersion,
                                                                      toVersion, ex.toString()));
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
    return conf;
  }

}
