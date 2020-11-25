/*
 * Copyright 2018 StreamSets Inc.
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineFragmentConfiguration;
import com.streamsets.datacollector.config.SparkClusterType;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.configupgrade.FragmentConfigurationUpgrader;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.impl.FilePipelineStoreTask;
import com.streamsets.datacollector.util.ElUtil;
import com.streamsets.datacollector.util.PipelineConfigurationUtil;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageType;
import org.apache.commons.collections.CollectionUtils;
import org.jparsec.internal.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@SuppressWarnings("Duplicates")
public class PipelineFragmentConfigurationValidator {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineFragmentConfigurationValidator.class);
  static final String TO_ERROR_NULL_TARGET = "com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget";

  protected final StageLibraryTask stageLibrary;
  protected final String name;
  private PipelineFragmentConfiguration pipelineFragmentConfiguration;
  protected final String user;
  protected final Map<String, ConnectionConfiguration> connections;
  protected final Issues issues;
  private final List<String> openLanes;
  boolean validated;
  protected boolean canPreview;
  protected final Map<String, Object> constants;
  private boolean isPipelineFragment = false;
  protected final BuildInfo buildInfo;

  public PipelineFragmentConfigurationValidator(
      StageLibraryTask stageLibrary,
      BuildInfo buildInfo,
      String name,
      PipelineFragmentConfiguration pipelineFragmentConfiguration,
      String user,
      Map<String, ConnectionConfiguration> connections
  ) {
    this.stageLibrary = Preconditions.checkNotNull(stageLibrary, "stageLibrary cannot be null");
    this.buildInfo = buildInfo;
    this.name = Preconditions.checkNotNull(name, "name cannot be null");
    this.pipelineFragmentConfiguration = Preconditions.checkNotNull(
        pipelineFragmentConfiguration,
        "pipelineFragmentConfiguration cannot be null"
    );
    this.user = user;
    this.connections = connections;
    issues = new Issues();
    openLanes = new ArrayList<>();
    this.constants = ElUtil.getConstants(pipelineFragmentConfiguration.getConfiguration());
  }

  boolean sortStages(boolean sortOriginalStages) {
    boolean ok = true;
    List<StageConfiguration> original;

    if (sortOriginalStages) {
      original = new ArrayList<>(pipelineFragmentConfiguration.getOriginalStages());
    } else {
      original = new ArrayList<>(pipelineFragmentConfiguration.getStages());
    }

    List<StageConfiguration> sorted = new ArrayList<>();
    Set<String> producedOutputs = new HashSet<>();
    while (ok && !original.isEmpty()) {
      int prior = sorted.size();
      Iterator<StageConfiguration> it = original.iterator();
      while (it.hasNext()) {
        StageConfiguration stage = it.next();
        if (producedOutputs.containsAll(stage.getInputLanes())) {
          producedOutputs.addAll(stage.getOutputAndEventLanes());
          it.remove();
          sorted.add(stage);
        }
      }
      if (prior == sorted.size()) {
        // pipeline has not stages at all
        List<String> names = new ArrayList<>(original.size());
        for (StageConfiguration stage : original) {
          names.add(stage.getInstanceName());
        }
        issues.add(IssueCreator.getPipeline().create(ValidationError.VALIDATION_0002, names));
        ok = false;
      }
    }
    sorted.addAll(original);

    if (sortOriginalStages) {
      pipelineFragmentConfiguration.setOriginalStages(sorted);
    } else {
      pipelineFragmentConfiguration.setStages(sorted);
    }
    return ok;
  }

  public PipelineFragmentConfiguration validateFragment() {
    Preconditions.checkState(!validated, "Already validated");
    isPipelineFragment = true;
    validated = true;
    LOG.trace("Pipeline '{}' starting validation", name);
    resolveLibraryAliases();

    // We want to run addMissingConfigs only if upgradePipeline was a success to not perform any side-effects when the
    // upgrade is not successful.
    canPreview = upgradePipelineFragment() && addPipelineFragmentMissingConfigs();
    canPreview &= sortStages(false);
    if (CollectionUtils.isNotEmpty(pipelineFragmentConfiguration.getFragments())) {
      canPreview &= sortStages(true);
    }
    canPreview &= checkIfPipelineIsEmpty();
    canPreview &= loadAndValidatePipelineFragmentConfig();
    canPreview &= validateStageConfiguration();
    canPreview &= validatePipelineLanes();
    canPreview &= validateTestOriginStage();
    canPreview &= validateEventAndDataLanesDoNotCross();
    canPreview &= validateStagesExecutionMode(pipelineFragmentConfiguration);
    canPreview &= validateCommitTriggerStage(pipelineFragmentConfiguration);

    if (LOG.isTraceEnabled() && issues.hasIssues()) {
      for (Issue issue : issues.getPipelineIssues()) {
        LOG.trace("Pipeline Fragment '{}', {}", name, issue);
      }
      for (Issue issue : issues.getIssues()) {
        LOG.trace("Pipeline Fragment '{}', {}", name, issue);
      }
    }
    LOG.debug(
        "Pipeline Fragment '{}' validation. valid={}, canPreview={}, issuesCount={}",
        name,
        !issues.hasIssues(),
        canPreview,
        issues.getIssueCount()
    );

    pipelineFragmentConfiguration.setValidation(this);
    return pipelineFragmentConfiguration;
  }

  protected void resolveLibraryAliases() {
    ValidationUtil.resolveLibraryAliases(stageLibrary, pipelineFragmentConfiguration.getStages());
  }

  FragmentConfigurationUpgrader getFragmentUpgrader() {
    return FragmentConfigurationUpgrader.get();
  }

  private boolean upgradePipelineFragment() {
    List<Issue> upgradeIssues = new ArrayList<>();

    PipelineFragmentConfiguration fConf = getFragmentUpgrader().upgradeIfNecessary(
        stageLibrary,
        buildInfo,
        pipelineFragmentConfiguration,
        upgradeIssues
    );
    if (fConf != null) {
      pipelineFragmentConfiguration = fConf;
    }

    issues.addAll(upgradeIssues);
    return upgradeIssues.isEmpty();
  }

  private boolean addPipelineFragmentMissingConfigs() {
    for (ConfigDefinition configDef : stageLibrary.getPipelineFragment().getConfigDefinitions()) {
      String configName = configDef.getName();
      Config config = pipelineFragmentConfiguration.getConfiguration(configName);
      if (config == null) {
        Object defaultValue = configDef.getDefaultValue();
        LOG.warn("Pipeline missing configuration '{}', adding with '{}' as default", configName, defaultValue);
        config = new Config(configName, defaultValue);
        pipelineFragmentConfiguration.addConfiguration(config);
      }
    }

    addMissingConfigs();

    return true;
  }

  protected void addMissingConfigs() {
    for (StageConfiguration stageConf : pipelineFragmentConfiguration.getStages()) {
      ValidationUtil.addMissingConfigsToStage(stageLibrary, stageConf);
    }
  }

  boolean validateStageExecutionMode(
      StageConfiguration stageConf,
      ExecutionMode executionMode,
      List<Issue> issues,
      String configGroup,
      String configName
  ) {
    boolean canPreview = true;
    IssueCreator issueCreator = IssueCreator.getStage(stageConf.getInstanceName());
    StageDefinition stageDef = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(), false);
    if (stageDef != null) {
      if (!stageDef.getExecutionModes().contains(executionMode)) {
        canPreview = false;
        if (configGroup != null && configName != null) {
          issues.add(
              IssueCreator.getPipeline().create(
                  configGroup,
                  configName,
                  ValidationError.VALIDATION_0071,
                  stageDef.getLabel(),
                  stageDef.getLibraryLabel(),
                  executionMode.getLabel(),
                  stageDef.getExecutionModes().stream().map(ExecutionMode::getLabel).collect(Collectors.joining(", "))
              )
          );
        } else {
          issues.add(
              issueCreator.create(
                  ValidationError.VALIDATION_0071,
                  stageDef.getLabel(),
                  stageDef.getLibraryLabel(),
                  executionMode.getLabel(),
                  stageDef.getExecutionModes().stream().map(ExecutionMode::getLabel).collect(Collectors.joining(", "))
              )
          );
        }
      }
    } else {
      canPreview = false;
      // StageDef missing is already handled in validateStageConfiguration, no need to issue the same error again
    }
    return canPreview;
  }

  private boolean validateStagesExecutionMode(PipelineFragmentConfiguration pipelineConf) {
    boolean canPreview = true;
    List<Issue> errors = new ArrayList<>();
    ExecutionMode pipelineExecutionMode = PipelineBeanCreator.get().getExecutionMode(pipelineConf, errors);
    if (errors.isEmpty()) {
      for (StageConfiguration stageConf : pipelineConf.getStages()) {
        canPreview &= validateStageExecutionMode(stageConf, pipelineExecutionMode, errors, null, null);
      }
    } else {
      canPreview = false;
    }
    issues.addAll(errors);
    return canPreview;
  }

  boolean validateStageLibraryClusterType(
      StageConfiguration stageConf,
      SparkClusterType clusterType,
      List<Issue> issues,
      String configGroup,
      String configName
  ) {
    boolean canPreview = true;
    IssueCreator issueCreator = IssueCreator.getStage(stageConf.getInstanceName());
    StageLibraryDefinition stageLibraryDef = stageLibrary.getStageLibraryDefinition(stageConf.getLibrary());
    StageDefinition stageDef = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(), false);
    if (stageDef != null && stageLibraryDef != null) {
      if (stageLibraryDef.getClusterTypes() != null && !stageLibraryDef.getClusterTypes().contains(clusterType)) {
        canPreview = false;
        if (configGroup != null && configName != null) {
          issues.add(
              IssueCreator.getPipeline().create(
                  configGroup,
                  configName,
                  ValidationError.VALIDATION_0300,
                  stageDef.getLabel(),
                  stageDef.getLibraryLabel(),
                  clusterType.getLabel(),
                  Strings.join(", ", stageLibraryDef.getClusterTypes().stream().map(SparkClusterType::getLabel).toArray())
              )
          );
        } else {
          issues.add(
              issueCreator.create(
                  ValidationError.VALIDATION_0300,
                  stageDef.getLabel(),
                  stageDef.getLibraryLabel(),
                  clusterType.getLabel(),
                  Strings.join(", ", stageLibraryDef.getClusterTypes().stream().map(SparkClusterType::getLabel).toArray())
              )
          );
        }
      }
    } else if (stageDef == null) {
      canPreview = false;
      // StageDef missing is already handled in validateStageConfiguration, no need to issue the same error again
    }
    return canPreview;
  }

  private boolean loadAndValidatePipelineFragmentConfig() {
    List<Issue> errors = new ArrayList<>();

    // to validate EL values in the pipeline and stage configuration create Pipeline bean from Pipeline Fragment
    StageConfiguration errorStageInstance = PipelineConfigurationUtil.getStageConfigurationWithDefaultValues(
        stageLibrary,
        PipelineConfigBean.DEFAULT_STATS_AGGREGATOR_LIBRARY_NAME,
        TO_ERROR_NULL_TARGET,
        "errorStageInstance",
        "Error Stage"
    );
    if (errorStageInstance != null) {
      errorStageInstance.setOutputLanes(
          ImmutableList.of(errorStageInstance.getInstanceName() + "OutputLane1")
      );
    }
    PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(
        FilePipelineStoreTask.SCHEMA_VERSION,
        PipelineConfigBean.VERSION,
        pipelineFragmentConfiguration.getPipelineId(),
        pipelineFragmentConfiguration.getUuid(),
        pipelineFragmentConfiguration.getTitle(),
        pipelineFragmentConfiguration.getDescription(),
        new ArrayList<>(pipelineFragmentConfiguration.getConfiguration()),
        Collections.emptyMap(),
        null,
        pipelineFragmentConfiguration.getStages(),
        errorStageInstance,
        null,
        Collections.emptyList(),
        Collections.emptyList(),
        null
    );
    PipelineBeanCreator.get().create(
        false,
        stageLibrary,
        pipelineConfiguration,
        null,
        user,
        connections,
        errors
    );
    if (pipelineFragmentConfiguration.getTitle() != null && pipelineFragmentConfiguration.getTitle().isEmpty()) {
      issues.add(IssueCreator.getPipeline().create(ValidationError.VALIDATION_0093));
    }
    issues.addAll(errors);
    return errors.isEmpty();
  }

  public boolean canPreview() {
    Preconditions.checkState(validated, "validate() has not been called");
    return canPreview;
  }

  public Issues getIssues() {
    Preconditions.checkState(validated, "validate() has not been called");
    return issues;
  }

  public List<String> getOpenLanes() {
    Preconditions.checkState(validated, "validate() has not been called");
    return openLanes;
  }

  boolean checkIfPipelineIsEmpty() {
    boolean preview = true;
    if (pipelineFragmentConfiguration.getStages().isEmpty()) {
      // pipeline has not stages at all
      issues.add(IssueCreator.getPipeline().create(ValidationError.VALIDATION_0001));
      preview = false;
    }
    return preview;
  }

  @VisibleForTesting
  boolean validateStageConfiguration() {
    boolean preview = true;
    Set<String> stageNames = new HashSet<>();
    boolean shouldBeSource = true;
    for (StageConfiguration stageConf : pipelineFragmentConfiguration.getStages()) {
      if (stageNames.contains(stageConf.getInstanceName())) {
        // duplicate stage instance name in the pipeline
        issues.add(
            IssueCreator.getStage(stageConf.getInstanceName())
                .create(stageConf.getInstanceName(),
                    ValidationError.VALIDATION_0005
                )
        );
        preview = false;
      }
      preview &= validateStageConfiguration(
          shouldBeSource,
          stageConf,
          false,
          IssueCreator.getStage(stageConf.getInstanceName())
      );
      stageNames.add(stageConf.getInstanceName());
      shouldBeSource = false;
    }
    return preview;
  }

  protected boolean validateStageConfiguration(
    boolean shouldBeSource,
    StageConfiguration stageConf,
    boolean noInputAndEventLanes,
    IssueCreator issueCreator
  ) {
    List<Issue> errors = new ArrayList<>();
    boolean preview = ValidationUtil.validateStageConfiguration(
        stageLibrary,
        shouldBeSource,
        stageConf,
        noInputAndEventLanes,
        issueCreator,
        isPipelineFragment,
        constants,
        errors
    );
    this.issues.addAll(errors);

    return preview;
  }



  @VisibleForTesting
  boolean validatePipelineLanes() {
    boolean preview = true;
    List<StageConfiguration> stagesConf = pipelineFragmentConfiguration.getStages();

    if (CollectionUtils.isNotEmpty(this.pipelineFragmentConfiguration.getFragments())) {
      stagesConf = pipelineFragmentConfiguration.getOriginalStages();
    }

    for (int i = 0; i < stagesConf.size(); i++) {
      StageConfiguration stageConf = stagesConf.get(i);

      Set<String> openOutputs = new LinkedHashSet<>(stageConf.getOutputLanes());
      Set<String> openEvents = new LinkedHashSet<>(stageConf.getEventLanes());

      for (int j = i + 1; j < stagesConf.size(); j++) {
        StageConfiguration downStreamStageConf = stagesConf.get(j);

        Set<String> duplicateOutputs = Sets.intersection(
            new HashSet<>(stageConf.getOutputLanes()),
            new HashSet<>(downStreamStageConf.getOutputLanes())
        );

        Set<String> duplicateEvents = Sets.intersection(
            new HashSet<>(stageConf.getEventLanes()),
            new HashSet<>(downStreamStageConf.getEventLanes())
        );

        if (!duplicateOutputs.isEmpty()) {
          // there is more than one stage defining the same output lane
          issues.add(IssueCreator
              .getPipeline()
              .create(
                  downStreamStageConf.getInstanceName(),
                  ValidationError.VALIDATION_0010,
                  duplicateOutputs, stageConf.getInstanceName()
              )
          );
          preview = false;
        }

        if (!duplicateEvents.isEmpty()) {
          // there is more than one stage defining the same output lane
          issues.add(IssueCreator
              .getPipeline()
              .create(
                  downStreamStageConf.getInstanceName(),
                  ValidationError.VALIDATION_0010,
                  duplicateEvents, stageConf.getInstanceName()
              )
          );
          preview = false;
        }

        // How many lanes are shared between the two stages
        Set<String> sharedLanes = Sets.intersection(
            new HashSet<>(stageConf.getOutputLanes()),
            new HashSet<>(downStreamStageConf.getInputLanes())
        );
        if(sharedLanes.size() > 1) {
          // More then one lane connecting the two stages
          issues.add(IssueCreator
              .getPipeline()
              .create(
                  downStreamStageConf.getInstanceName(),
                  ValidationError.VALIDATION_0039,
                  stageConf.getInstanceName(),
                  downStreamStageConf.getInstanceName()
              )
          );
          preview = false;
        }

        openOutputs.removeAll(downStreamStageConf.getInputLanes());
        openEvents.removeAll(downStreamStageConf.getInputLanes());
      }
      if (!openOutputs.isEmpty() && !isPipelineFragment) {
        openLanes.addAll(openOutputs);
        // the stage has open output lanes
        Issue issue = IssueCreator.getStage(stageConf.getInstanceName()).create(ValidationError.VALIDATION_0011);
        issue.setAdditionalInfo("openStreams", openOutputs);
        issues.add(issue);
      }

      if (!openEvents.isEmpty() && !isPipelineFragment) {
        openLanes.addAll(openEvents);
        // the stage has open Event lanes
        Issue issue = IssueCreator.getStage(stageConf.getInstanceName()).create(ValidationError.VALIDATION_0104);
        issue.setAdditionalInfo("openStreams", openEvents);
        issues.add(issue);
      }


    }
    return preview;
  }

  @VisibleForTesting
  boolean validateEventAndDataLanesDoNotCross() {
    // We know that the pipeline is sorted at this point (e.g. all stages that are producing data for a given stage
    // appear before that stage in the list).
    List<StageConfiguration> stagesConf = pipelineFragmentConfiguration.getStages();
    if(stagesConf.size() < 1) {
      return true; // We have nothing to validate
    }

    // First stage is always on the data path
    Set<String> eventLanes = new HashSet<>(stagesConf.get(0).getEventLanes());
    Set<String> dataLanes = new HashSet<>(stagesConf.get(0).getOutputLanes());

    for (int i = 1; i < stagesConf.size(); i++) {
      StageConfiguration stageConf = stagesConf.get(i);

      boolean isEventStage = false;
      boolean isDataStage = false;
      for(String inputStage : stageConf.getInputLanes()) {
        if(eventLanes.contains(inputStage)) {
          isEventStage = true;
        }
        if(dataLanes.contains(inputStage)) {
          isDataStage = true;
        }
      }

      // We're ignoring state where the stage is not on event nor on data path - that means that the component is not
      // connected anywhere and that means that previous checks already flagged this scenario.

      if(isEventStage && isDataStage) {
        issues.add(IssueCreator.getPipeline().create(
          ValidationError.VALIDATION_0103,
          stageConf.getInstanceName()
        ));
        return false;
      }

      if(isEventStage) {
        eventLanes.addAll(stageConf.getOutputLanes());
      } else {
        dataLanes.addAll(stageConf.getOutputLanes());
      }

      // Persist the information if this is event stage in it's configuration
      stageConf.setInEventPath(isEventStage);

      // Event lane always feeds records to event part of the pipeline
      eventLanes.addAll(stageConf.getEventLanes());
    }

    return true;
  }

  boolean validateCommitTriggerStage(PipelineFragmentConfiguration pipelineFragmentConfiguration) {
    boolean valid = true;
    StageConfiguration target = null;
    int offsetCommitTriggerCount = 0;
    // Count how many targets can trigger offset commit in this pipeline
    for (StageConfiguration stageConf : pipelineFragmentConfiguration.getStages()) {
      StageDefinition stageDefinition = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(), false);
      if (stageDefinition != null) {
        if (stageDefinition.getType() == StageType.TARGET && stageDefinition.isOffsetCommitTrigger()) {
          target = stageConf;
          offsetCommitTriggerCount++;
        }
      } else {
        valid = false;
        // Validation for missing StageDef is already in validateStageConfiguration(), not producing duplicate errors
      }
    }
    // If a pipeline contains a target that triggers offset commit then,
    // 1. delivery guarantee must be AT_LEAST_ONCE
    // 2. the pipeline can have only one target that triggers offset commit
    if (offsetCommitTriggerCount == 1) {
      Config deliveryGuarantee = pipelineFragmentConfiguration.getConfiguration("deliveryGuarantee");
      Object value = deliveryGuarantee.getValue();
      if (!DeliveryGuarantee.AT_LEAST_ONCE.name().equals(String.valueOf(value))) {
        IssueCreator issueCreator = IssueCreator.getStage(target.getInstanceName());
        issues.add(issueCreator.create(ValidationError.VALIDATION_0092, DeliveryGuarantee.AT_LEAST_ONCE));
      }
    } else if (offsetCommitTriggerCount > 1) {
      IssueCreator issueCreator = IssueCreator.getPipeline();
      issues.add(issueCreator.create(ValidationError.VALIDATION_0091));
    }
    return valid;
  }

  public static String getStageDefQualifiedName(String library, String stageName, String stageVersion) {
    return library + "::" + stageName + "::" + stageVersion;
  }

  public static String[] getSpecialStageDefQualifiedNameParts(String stageQualifiedName) {
    return stageQualifiedName.split("::");
  }

  boolean validateTestOriginStage() {
    boolean preview = true;
    StageConfiguration testOriginStage = pipelineFragmentConfiguration.getTestOriginStage();
    if (testOriginStage != null) {
      IssueCreator errorStageCreator = IssueCreator.getStage(testOriginStage.getInstanceName());
      List<Issue> errors = new ArrayList<>();
      preview = ValidationUtil.validateStageConfiguration(
        stageLibrary,
        true,
        testOriginStage,
        true,
        errorStageCreator,
        isPipelineFragment,
        constants,
        errors
      );
      this.issues.addAll(errors);
    }
    return preview;
  }
}
