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
package com.streamsets.datacollector.validation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineGroups;
import com.streamsets.datacollector.config.ServiceConfiguration;
import com.streamsets.datacollector.config.ServiceDependencyDefinition;
import com.streamsets.datacollector.config.SparkClusterType;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.configupgrade.PipelineConfigurationUpgrader;
import com.streamsets.datacollector.creation.PipelineBean;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.ServiceBean;
import com.streamsets.datacollector.creation.StageBean;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageType;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("Duplicates")
public class PipelineConfigurationValidator extends PipelineFragmentConfigurationValidator {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigurationValidator.class);

  private final Configuration dataCollectorConfiguration;
  private final RuntimeInfo runtimeInfo;

  private PipelineConfiguration pipelineConfiguration;
  private PipelineBean pipelineBean;

  public PipelineConfigurationValidator(
      StageLibraryTask stageLibrary,
      BuildInfo buildInfo,
      String name,
      PipelineConfiguration pipelineConfiguration,
      String user,
      Map<String, ConnectionConfiguration> connections
  ) {
    this(stageLibrary, buildInfo, name, pipelineConfiguration, null, null, user, connections);
  }

  public PipelineConfigurationValidator(
      StageLibraryTask stageLibrary,
      BuildInfo buildInfo,
      String name,
      PipelineConfiguration pipelineConfiguration,
      Configuration dataCollectorConfiguration,
      RuntimeInfo runtimeInfo,
      String user,
      Map<String, ConnectionConfiguration> connections
  ) {
    super(stageLibrary, buildInfo, name, pipelineConfiguration, user, connections);
    this.pipelineConfiguration = pipelineConfiguration;
    this.dataCollectorConfiguration = dataCollectorConfiguration;
    this.runtimeInfo = runtimeInfo;
  }

  public PipelineConfiguration validate() {
    Preconditions.checkState(!validated, "Already validated");
    validated = true;
    LOG.trace("Pipeline '{}' starting validation", name);
    resolveLibraryAliases();


    // We want to run addMissingConfigs only if upgradePipeline was a success to not perform any side-effects when the
    // upgrade is not successful.
    canPreview = upgradePipeline() && addPipelineMissingConfigs();
    canPreview &= sortStages(false);
    if (CollectionUtils.isNotEmpty(pipelineConfiguration.getFragments())) {
      canPreview &= sortStages(true);
    }
    canPreview &= checkIfPipelineIsEmpty();
    canPreview &= loadAndValidatePipelineConfig();
    canPreview &= validateStageConfiguration();
    canPreview &= validatePipelineLanes();
    canPreview &= validateEventAndDataLanesDoNotCross();
    canPreview &= validateErrorStage();
    canPreview &= validateTestOriginStage();
    canPreview &= validateStatsAggregatorStage();
    canPreview &= validatePipelineLifecycleEvents();
    canPreview &= validateStagesExecutionMode(pipelineConfiguration);
    canPreview &= validateBisectability(pipelineConfiguration);
    canPreview &= validateCommitTriggerStage(pipelineConfiguration);

    if (!issues.hasIssues()) {
      // if every thing is good, check for disconnected data flows to support partial preview
      canPreview &= checkForDisconnectedDataFlows();
    }

    upgradeBadRecordsHandlingStage(pipelineConfiguration);
    upgradeStatsAggregatorStage(pipelineConfiguration);
    propagateRuntimeConfiguration();

    if (LOG.isTraceEnabled() && issues.hasIssues()) {
      for (Issue issue : issues.getPipelineIssues()) {
        LOG.trace("Pipeline '{}', {}", name, issue);
      }
      for (Issue issue : issues.getIssues()) {
        LOG.trace("Pipeline '{}', {}", name, issue);
      }
    }
    LOG.debug(
        "Pipeline '{}' validation. valid={}, canPreview={}, issuesCount={}",
        name,
        !issues.hasIssues(),
        canPreview,
        issues.getIssueCount()
    );

    pipelineConfiguration.setValidation(this);
    return pipelineConfiguration;
  }

  private boolean checkForDisconnectedDataFlows() {
    // All pipeline's stages that we have not visited yet
    List<StageConfiguration> stages = new LinkedList<>(pipelineConfiguration.getStages());

    // Starting point is the first stage
    StageConfiguration firstStage = stages.remove(0);

    Set<String> activeLanes = new HashSet<>();
    activeLanes.addAll(firstStage.getInputLanes());
    activeLanes.addAll(firstStage.getOutputAndEventLanes());
    Set<String> processedLanes = new HashSet<>(activeLanes);

    while(!activeLanes.isEmpty()) {
      Set<String> newActiveLanes = new HashSet<>();
      List<StageConfiguration> toBeRemoved = new LinkedList<>();

      for(StageConfiguration stageConf : stages) {
        // All stage's lanes
        Set<String> lanes = Stream.of(stageConf.getInputLanes(), stageConf.getOutputAndEventLanes())
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());

        // This stage is not reachable
        if(Sets.intersection(activeLanes, lanes).isEmpty()) {
          continue;
        }

        // We have visited this stage
        toBeRemoved.add(stageConf);

        // Let's add the non-processed lanes to our iteration
        Set<String> newLanes = Sets.difference(lanes, processedLanes).immutableCopy();
        processedLanes.addAll(newLanes);
        newActiveLanes.addAll(newLanes);
      }

      stages.removeAll(toBeRemoved);
      activeLanes = newActiveLanes;
    }

    if(!stages.isEmpty()) {
      issues.add(IssueCreator.getPipeline().create(
        ValidationError.VALIDATION_0002,
        stages.stream().map(StageConfiguration::getInstanceName).collect(Collectors.toList())
      ));
      return false;
    }

    return true;
  }

  protected void resolveLibraryAliases() {
    // This will resolve all stages inside the pipeline canvas
    super.resolveLibraryAliases();

    List<StageConfiguration> stageConfigurations = new ArrayList<>();

    if(pipelineConfiguration.getStatsAggregatorStage() != null) {
      stageConfigurations.add(pipelineConfiguration.getStatsAggregatorStage());
    }
    if(pipelineConfiguration.getErrorStage() != null) {
      stageConfigurations.add(pipelineConfiguration.getErrorStage());
    }

    stageConfigurations.addAll(pipelineConfiguration.getStartEventStages());
    stageConfigurations.addAll(pipelineConfiguration.getStopEventStages());

    ValidationUtil.resolveLibraryAliases(stageLibrary, stageConfigurations);
  }

  @VisibleForTesting
  PipelineConfigurationUpgrader getUpgrader() {
    return PipelineConfigurationUpgrader.get();
  }

  private boolean upgradePipeline() {
    List<Issue> upgradeIssues = new ArrayList<>();

    PipelineConfiguration pConf = getUpgrader().upgradeIfNecessary(
        stageLibrary,
        buildInfo,
        pipelineConfiguration,
        upgradeIssues
    );
    if (pConf != null) {
      pipelineConfiguration = pConf;
    }

    issues.addAll(upgradeIssues);
    return upgradeIssues.isEmpty();
  }

  private boolean addPipelineMissingConfigs() {
    for (ConfigDefinition configDef : stageLibrary.getPipeline().getConfigDefinitions()) {
      String configName = configDef.getName();
      Config config = pipelineConfiguration.getConfiguration(configName);
      if (config == null) {
        Object defaultValue = configDef.getDefaultValue();
        LOG.warn("Pipeline missing configuration '{}', adding with '{}' as default", configName, defaultValue);
        config = new Config(configName, defaultValue);
        pipelineConfiguration.addConfiguration(config);
      }
    }

    addMissingConfigs();

    if(pipelineConfiguration.getErrorStage() != null) {
      ValidationUtil.addMissingConfigsToStage(stageLibrary, pipelineConfiguration.getErrorStage());
    }

    if(pipelineConfiguration.getStatsAggregatorStage() != null) {
      ValidationUtil.addMissingConfigsToStage(stageLibrary, pipelineConfiguration.getStatsAggregatorStage());
    }

    for(StageConfiguration stageConfiguration : pipelineConfiguration.getStartEventStages()) {
      ValidationUtil.addMissingConfigsToStage(stageLibrary, stageConfiguration);
    }

    for(StageConfiguration stageConfiguration : pipelineConfiguration.getStopEventStages()) {
      ValidationUtil.addMissingConfigsToStage(stageLibrary, stageConfiguration);
    }

    if(pipelineConfiguration.getTestOriginStage() != null) {
      ValidationUtil.addMissingConfigsToStage(stageLibrary, pipelineConfiguration.getTestOriginStage());
    }

    return true;
  }

  private boolean validateStagesExecutionMode(PipelineConfiguration pipelineConf) {
    boolean canPreview = true;
    List<Issue> errors = new ArrayList<>();

    ExecutionMode pipelineExecutionMode = PipelineBeanCreator.get().getExecutionMode(pipelineConf, errors);
    if (errors.isEmpty()) {
      StageConfiguration errorStage = pipelineConf.getErrorStage();
      if (errorStage != null) {
        canPreview &= validateStageExecutionMode(
            errorStage,
            pipelineExecutionMode,
            errors,
            PipelineGroups.BAD_RECORDS.name(),
            "badRecordsHandling"
        );
      }
      StageConfiguration statsStage = pipelineConf.getStatsAggregatorStage();
      if (statsStage != null) {
        canPreview &= validateStageExecutionMode(statsStage,
            pipelineExecutionMode,
            errors,
            PipelineGroups.STATS.name(),
            "statsAggregatorStage"
        );
      }
      for (StageConfiguration stageConf : pipelineConf.getStages()) {
        canPreview &= validateStageExecutionMode(stageConf, pipelineExecutionMode, errors, null, null);

        if (pipelineExecutionMode.equals(ExecutionMode.BATCH) ||
            pipelineExecutionMode.equals(ExecutionMode.STREAMING)) {
          // validate Stage library cluster type for Batch and Streaming mode
          SparkClusterType clusterType = PipelineBeanCreator.get().getClusterType(pipelineConf, errors);
          canPreview &= validateStageLibraryClusterType(
              stageConf,
              clusterType,
              errors,
              null,
              null
          );
        }
      }
    } else {
      canPreview = false;
    }

    issues.addAll(errors);
    return canPreview;
  }


  public boolean validateBisectability(PipelineConfiguration pipelineConf) {
    boolean canPreview = true;
    if (pipelineConf.getConfiguration("advancedErrorHandling") != null &&
        (boolean) pipelineConf.getConfiguration("advancedErrorHandling").getValue()) {
      for (StageConfiguration stageConf : pipelineConf.getStages()) {
        StageDefinition stageDef = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(), false);
        if (isOrigin(stageDef) && !stageDef.isBisectable()) {
          issues.add(IssueCreator.getPipeline().create(
              ValidationError.VALIDATION_0074,
              stageConf.getInstanceName(),
              stageDef.getClassName()
          ));
          canPreview = false;
        }
      }
    }
    return canPreview;
  }

  private boolean isOrigin(StageDefinition stageDef) {
    return stageDef.getType() == StageType.SOURCE;
  }


  private boolean loadAndValidatePipelineConfig() {
    List<Issue> errors = new ArrayList<>();

    pipelineBean = PipelineBeanCreator.get().create(
      false,
      stageLibrary,
      pipelineConfiguration,
      null,
      user,
      connections,
      errors
    );
    StageConfiguration pipelineConfs = PipelineBeanCreator.getPipelineConfAsStageConf(pipelineConfiguration);
    IssueCreator issueCreator = IssueCreator.getPipeline();
    for (ConfigDefinition confDef : PipelineBeanCreator.PIPELINE_DEFINITION.getConfigDefinitions()) {
      Config config = pipelineConfs.getConfig(confDef.getName());
      // No need to validate bad records, its validated before in PipelineBeanCreator.create()
      if (!confDef.getGroup().equals(PipelineGroups.BAD_RECORDS.name()) && confDef.isRequired()
        && (config == null || ValidationUtil.isNullOrEmpty(confDef, config))) {
        ValidationUtil.validateRequiredField(confDef, pipelineConfs, issueCreator, errors);
      }
      if (confDef.getType() == ConfigDef.Type.NUMBER && !ValidationUtil.isNullOrEmpty(confDef, config)) {
        ValidationUtil.validatedNumberConfig(config, confDef, pipelineConfs, issueCreator, errors);
      }
    }

    if (pipelineConfiguration.getTitle() != null && pipelineConfiguration.getTitle().isEmpty()) {
      issues.add(IssueCreator.getPipeline().create(ValidationError.VALIDATION_0093));
    }

    if (dataCollectorConfiguration != null && runtimeInfo != null && pipelineBean != null) {
      ValidationUtil.validateClusterConfigs(
          pipelineBean,
          dataCollectorConfiguration,
          runtimeInfo,
          issueCreator,
          errors
      );
    }

    issues.addAll(errors);
    return errors.isEmpty();
  }

  @VisibleForTesting
  boolean validateErrorStage() {
    boolean preview = true;
    StageConfiguration errorStage = pipelineConfiguration.getErrorStage();
    if (errorStage != null) {
      IssueCreator errorStageCreator = IssueCreator.getStage(errorStage.getInstanceName());
      preview = validateStageConfiguration(false, errorStage, true, errorStageCreator);
    }
    return preview;
  }

  @VisibleForTesting
  boolean validateStatsAggregatorStage() {
    boolean preview = true;
    StageConfiguration statsAggregatorStage = pipelineConfiguration.getStatsAggregatorStage();
    if (statsAggregatorStage != null) {
      IssueCreator errorStageCreator = IssueCreator.getStage(statsAggregatorStage.getInstanceName());
      preview = validateStageConfiguration(false, statsAggregatorStage, true, errorStageCreator);
    }
    return preview;
  }

  private boolean validatePipelineLifecycleEvents() {
    boolean preview = true;

    // Pipeline lifecycle events are only supported in STANDALONE mode
    List<Issue> localIssues = new ArrayList<>();
    ExecutionMode pipelineExecutionMode = PipelineBeanCreator.get().getExecutionMode(pipelineConfiguration, localIssues);
    issues.addAll(localIssues);

    // Validate each start/stop event handlers
    preview &= validatePipelineLifecycleEventStages(pipelineConfiguration.getStartEventStages(), pipelineExecutionMode);
    preview &= validatePipelineLifecycleEventStages(pipelineConfiguration.getStopEventStages(), pipelineExecutionMode);

    return preview;
  }

  private boolean validatePipelineLifecycleEventStages(
      List<StageConfiguration> eventStages,
      ExecutionMode executionMode
  ) {
    if(eventStages == null) {
      issues.add(IssueCreator.getPipeline().create(
          ValidationError.VALIDATION_0105,
          "Definition can't be null"
      ));
      return false;
    }

    if(eventStages.size() > 1) {
      issues.add(IssueCreator.getPipeline().create(
          ValidationError.VALIDATION_0105,
          "Only one event stage is allowed"
      ));
      return false;
    }

    if(eventStages.size() == 1) {
      // Special exception for cluster pipelines - UI will always inject discard executor, so we need to ignore it
      if(executionMode != ExecutionMode.STANDALONE && !TO_ERROR_NULL_TARGET.equals(eventStages.get(0).getStageName())) {
        issues.add(IssueCreator.getPipeline().create(
            ValidationError.VALIDATION_0106,
            executionMode
        ));
        return false;
      }

      // Validate the stage configuration itself as it's specified properly
      IssueCreator errorStageCreator = IssueCreator.getStage(eventStages.get(0).getInstanceName());
      return validateStageConfiguration(false, eventStages.get(0), true, errorStageCreator);
    }

    return true;
  }

  private void upgradeBadRecordsHandlingStage(PipelineConfiguration pipelineFragmentConfiguration) {
    // If there are upgrades on Error Record Stage Lib, upgrade "badRecordsHandling" config value
    upgradeSpecialStage(pipelineFragmentConfiguration, "badRecordsHandling", pipelineFragmentConfiguration.getErrorStage());
  }

  private void upgradeStatsAggregatorStage(PipelineConfiguration pipelineFragmentConfiguration) {
    // If there are upgrades on Stats Aggregator Stage Lib, upgrade "badRecordsHandling" config value
    upgradeSpecialStage(
        pipelineFragmentConfiguration,
        "statsAggregatorStage",
        pipelineFragmentConfiguration.getStatsAggregatorStage()
    );
  }

  private void upgradeSpecialStage(
      PipelineConfiguration pipelineConfiguration,
      String label,
      StageConfiguration stageConfig
  ) {
    Config config = pipelineConfiguration.getConfiguration(label);
    final String stageName = stageConfig == null ? "" :
        getStageDefQualifiedName(
            stageConfig.getLibrary(),
            stageConfig.getStageName(),
            String.valueOf(stageConfig.getStageVersion())
        );
    if (!(config == null || config.getValue() == null|| config.getValue().equals(stageName))) {
      pipelineConfiguration.getConfiguration().remove(config);
      pipelineConfiguration.getConfiguration().add(new Config(label, stageName));
    }
  }

  /**
   * We have special type of a ConfigDef called RUNTIME. This config is never displayed in UI and instead it's values
   * are supplied at "runtime". This method is the "runtime" method that propagates them.
   */
  private void propagateRuntimeConfiguration() {
    // If pipeline wasn't loaded or there if there are no stages, there is nothing to propagate
    if(pipelineBean == null || pipelineBean.getPipelineStageBeans() == null) {
      return;
    }

    for(StageBean stageBean : pipelineBean.getPipelineStageBeans().getStages()) {
      for(ServiceDependencyDefinition serviceDependency: stageBean.getDefinition().getServices()) {

        ServiceBean stageService = stageBean.getService(serviceDependency.getServiceClass());
        if (stageService == null){
          continue;
        }
        ServiceConfiguration serviceConfiguration = stageService.getConf();
        List<Config> configs = serviceConfiguration.getConfiguration();

        // Simply remove all RUNTIME configs
        configs.removeAll(
            serviceDependency.getConfiguration().keySet().stream()
                .map(serviceConfiguration::getConfig)
                .collect(Collectors.toList())
        );

        // And insert them with the stage-instance-constant values
        serviceDependency.getConfiguration().forEach((key, value) -> configs.add(new Config(key, value)));

        // And overwrite the new state
        serviceConfiguration.setConfig(configs);
      }
    }
  }
}
