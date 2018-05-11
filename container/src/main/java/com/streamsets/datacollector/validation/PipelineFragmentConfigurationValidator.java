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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.PipelineFragmentConfiguration;
import com.streamsets.datacollector.config.ServiceConfiguration;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.config.UserConfigurable;
import com.streamsets.datacollector.configupgrade.PipelineConfigurationUpgrader;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.StageConfigBean;
import com.streamsets.datacollector.definition.ConcreteELDefinitionExtractor;
import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.record.PathElement;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.ElUtil;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.impl.TextUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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

  protected final StageLibraryTask stageLibrary;
  protected final String name;
  private PipelineFragmentConfiguration pipelineFragmentConfiguration;
  protected final Issues issues;
  private final List<String> openLanes;
  boolean validated;
  protected boolean canPreview;
  protected final Map<String, Object> constants;
  private boolean isPipelineFragment = false;

  public PipelineFragmentConfigurationValidator(
      StageLibraryTask stageLibrary,
      String name,
      PipelineFragmentConfiguration pipelineFragmentConfiguration
  ) {
    this.stageLibrary = Preconditions.checkNotNull(stageLibrary, "stageLibrary cannot be null");
    this.name = Preconditions.checkNotNull(name, "name cannot be null");
    this.pipelineFragmentConfiguration = Preconditions.checkNotNull(
        pipelineFragmentConfiguration,
        "pipelineFragmentConfiguration cannot be null"
    );
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
    canPreview = resolveLibraryAliases();
    // We want to run addMissingConfigs only if upgradePipeline was a success to not perform any side-effects when the
    // upgrade is not successful.
    canPreview &= upgradePipelineFragment() && addPipelineFragmentMissingConfigs();
    canPreview &= sortStages(false);
    if (CollectionUtils.isNotEmpty(pipelineFragmentConfiguration.getFragments())) {
      canPreview &= sortStages(true);
    }
    canPreview &= checkIfPipelineIsEmpty();
    canPreview &= loadAndValidatePipelineFragmentConfig();
    canPreview &= validateStageConfiguration();
    canPreview &= validatePipelineLanes();
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


  boolean isLibraryAlias(String name) {
    return stageLibrary.getLibraryNameAliases().containsKey(name);
  }

  String resolveLibraryAlias(String name) {
    return stageLibrary.getLibraryNameAliases().get(name);
  }

  void resolveStageAlias(StageConfiguration stageConf) {
    String aliasKey = Joiner.on(",").join(stageConf.getLibrary(), stageConf.getStageName());
    String aliasValue = Strings.nullToEmpty(stageLibrary.getStageNameAliases().get(aliasKey));
    if (LOG.isTraceEnabled()) {
      for (String key : stageLibrary.getStageNameAliases().keySet()) {
        LOG.trace("Stage Lib Alias: {} => {}", key, stageLibrary.getStageNameAliases().get(key));
      }
      LOG.trace("Looking for '{}' and found '{}'", aliasKey, aliasValue);
    }
    if (!aliasValue.isEmpty()) {
      List<String> alias = Splitter.on(",").splitToList(aliasValue);
      if (alias.size() == 2) {
        LOG.debug("Converting '{}' to '{}'", aliasKey, aliasValue);
        stageConf.setLibrary(alias.get(0));
        stageConf.setStageName(alias.get(1));
      } else {
        LOG.error("Malformed stage alias: '{}'", aliasValue);
      }
    }
  }

  protected boolean resolveLibraryAliases() {
    List<StageConfiguration> stageConfigurations = new ArrayList<>(pipelineFragmentConfiguration.getStages());
    for (StageConfiguration stageConf : stageConfigurations) {
      String name = stageConf.getLibrary();
      if (isLibraryAlias(name)) {
        stageConf.setLibrary(resolveLibraryAlias(name));
      }
      resolveStageAlias(stageConf);
    }
    return true;
  }

  @VisibleForTesting
  PipelineConfigurationUpgrader getUpgrader() {
    return PipelineConfigurationUpgrader.get();
  }

  private boolean upgradePipelineFragment() {
    // For Fragment Version 1 - no upgrade required
    return true;
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
    for (StageConfiguration stageConf : pipelineFragmentConfiguration.getStages()) {
      addMissingConfigsToStage(stageConf);
    }
    return true;
  }

  void addMissingConfigsToStage(StageConfiguration stageConf) {
    StageDefinition stageDef = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(), false);
    if (stageDef != null) {
      for (ConfigDefinition configDef : stageDef.getConfigDefinitions()) {
        String configName = configDef.getName();
        Config config = stageConf.getConfig(configName);
        if (config == null) {
          Object defaultValue = configDef.getDefaultValue();
          LOG.warn(
              "Stage '{}' missing configuration '{}', adding with '{}' as default",
              stageConf.getInstanceName(),
              configName,
              defaultValue
          );
          config = new Config(configName, defaultValue);
          stageConf.addConfig(config);
        }
      }
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
                  executionMode.getLabel()
              )
          );
        } else {
          issues.add(
              issueCreator.create(
                  ValidationError.VALIDATION_0071,
                  stageDef.getLabel(),
                  stageDef.getLibraryLabel(),
                  executionMode.getLabel()
              )
          );
        }
      }
    } else {
      canPreview = false;
      issues.add(
          issueCreator.create(
              ValidationError.VALIDATION_0006,
              stageConf.getLibrary(),
              stageConf.getStageName(),
              stageConf.getStageVersion()
          )
      );
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

  private boolean loadAndValidatePipelineFragmentConfig() {
    List<Issue> errors = new ArrayList<>();
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

  private Config getConfig(List<Config> configs, String name) {
    for (Config config : configs) {
      if (config.getName().equals(name)) {
        return config;
      }
    }
    return null;
  }

  boolean validateStageConfiguration(
      boolean shouldBeSource,
      StageConfiguration stageConf,
      boolean noInputAndEventLanes,
      IssueCreator issueCreator
  ) {
    boolean preview = true;
    StageDefinition stageDef = stageLibrary.getStage(
        stageConf.getLibrary(),
        stageConf.getStageName(),
        false
    );
    if (stageDef == null) {
      // stage configuration refers to an undefined stage definition
      issues.add(
          issueCreator.create(
              stageConf.getInstanceName(),
              ValidationError.VALIDATION_0006,
              stageConf.getLibrary(),
              stageConf.getStageName(),
              stageConf.getStageVersion()
          )
      );
      preview = false;
    } else {
      if (shouldBeSource) {
        if (stageDef.getType() != StageType.SOURCE && !isPipelineFragment) {
          // first stage must be a Source
          issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0003));
          preview = false;
        }
      } else {
        if (stageDef.getType() == StageType.SOURCE) {
          // no stage other than first stage can be a Source
          issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0004));
          preview = false;
        }
      }
      if (!stageConf.isSystemGenerated() && !TextUtils.isValidName(stageConf.getInstanceName())) {
        // stage instance name has an invalid name (it must match '[0-9A-Za-z_]+')
        issues.add(
            issueCreator.create(
                stageConf.getInstanceName(),
                ValidationError.VALIDATION_0016,
                TextUtils.VALID_NAME
            )
        );
        preview = false;
      }
      for (String lane : stageConf.getInputLanes()) {
        if (!TextUtils.isValidName(lane)) {
          // stage instance input lane has an invalid name (it must match '[0-9A-Za-z_]+')
          issues.add(
              issueCreator.create(
                  stageConf.getInstanceName(),
                  ValidationError.VALIDATION_0017,
                  lane,
                  TextUtils.VALID_NAME
              )
          );
          preview = false;
        }
      }
      for (String lane : stageConf.getOutputLanes()) {
        if (!TextUtils.isValidName(lane)) {
          // stage instance output lane has an invalid name (it must match '[0-9A-Za-z_]+')
          issues.add(
              issueCreator.create(
                  stageConf.getInstanceName(),
                  ValidationError.VALIDATION_0018,
                  lane,
                  TextUtils.VALID_NAME
              )
          );
          preview = false;
        }
      }
      for (String lane : stageConf.getEventLanes()) {
        if (!TextUtils.isValidName(lane)) {
          // stage instance output lane has an invalid name (it must match '[0-9A-Za-z_]+')
          issues.add(
              issueCreator.create(
                  stageConf.getInstanceName(),
                  ValidationError.VALIDATION_0100,
                  lane,
                  TextUtils.VALID_NAME
              )
          );
          preview = false;
        }
      }

      // Validate proper input/output lane configuration
      switch (stageDef.getType()) {
        case SOURCE:
          if (!stageConf.getInputLanes().isEmpty()) {
            // source stage cannot have input lanes
            issues.add(
                issueCreator.create(
                    stageConf.getInstanceName(),
                    ValidationError.VALIDATION_0012,
                    stageDef.getType(),
                    stageConf.getInputLanes()
                )
            );
            preview = false;
          }
          if (!stageDef.isVariableOutputStreams()) {
            // source stage must match the output stream defined in StageDef
            if (stageDef.getOutputStreams() != stageConf.getOutputLanes().size()) {
              issues.add(
                  issueCreator.create(
                      stageConf.getInstanceName(),
                      ValidationError.VALIDATION_0015,
                      stageDef.getOutputStreams(),
                      stageConf.getOutputLanes().size()
                  )
              );
            }
          } else if (stageConf.getOutputLanes().isEmpty()) {
            // source stage must have at least one output lane
            issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0032));
          }
          break;
        case PROCESSOR:
          if (stageConf.getInputLanes().isEmpty() && !isPipelineFragment) {
            // processor stage must have at least one input lane
            issues.add(
                issueCreator.create(
                    stageConf.getInstanceName(),
                    ValidationError.VALIDATION_0014,
                    "Processor"
                )
            );
            preview = false;
          }
          if (!stageDef.isVariableOutputStreams()) {
            // processor stage must match the output stream defined in StageDef
            if (stageDef.getOutputStreams() != stageConf.getOutputLanes().size()) {
              issues.add(
                  issueCreator.create(
                      stageConf.getInstanceName(),
                      ValidationError.VALIDATION_0015,
                      stageDef.getOutputStreams(),
                      stageConf.getOutputLanes().size()
                  )
              );
            }
          } else if (stageConf.getOutputLanes().isEmpty()) {
            // processor stage must have at least one output lane
            issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0032));
          }
          break;
        case EXECUTOR:
        case TARGET:
          // Normal target stage must have at least one input lane
          if (!noInputAndEventLanes && stageConf.getInputLanes().isEmpty() && !isPipelineFragment) {
            issues.add(
                issueCreator.create(
                    stageConf.getInstanceName(),
                    ValidationError.VALIDATION_0014,
                    "Target"
                )
            );
            preview = false;
          }
          // Error/Stats/Pipeline lifecycle must not have an input lane
          if (noInputAndEventLanes && !stageConf.getInputLanes().isEmpty()) {
            issues.add(
                issueCreator.create(
                    stageConf.getInstanceName(),
                    ValidationError.VALIDATION_0012,
                    "Error/Stats/Lifecycle",
                    stageConf.getInputLanes()
                )
            );
            preview = false;
          }
          if (!stageConf.getOutputLanes().isEmpty()) {
            // target stage cannot have output lanes
            issues.add(
                issueCreator.create(
                    stageConf.getInstanceName(),
                    ValidationError.VALIDATION_0013,
                    stageDef.getType(),
                    stageConf.getOutputLanes()
                )
            );
            preview = false;
          }
          if (noInputAndEventLanes && !stageConf.getEventLanes().isEmpty()) {
            issues.add(
                issueCreator.create(
                    stageConf.getInstanceName(),
                    ValidationError.VALIDATION_0036,
                    stageDef.getType(),
                    stageConf.getEventLanes()
                )
            );
            preview = false;
          }
          break;
        default:
          throw new IllegalStateException("Unexpected stage type " + stageDef.getType());
      }

      // Validate proper event configuration
      if(stageConf.getEventLanes().size() > 1) {
        issues.add(
          issueCreator.create(
            stageConf.getInstanceName(),
            ValidationError.VALIDATION_0101
          )
        );
        preview = false;
      }
      if(!stageDef.isProducingEvents() && stageConf.getEventLanes().size() > 0) {
        issues.add(
          issueCreator.create(
            stageConf.getInstanceName(),
            ValidationError.VALIDATION_0102
          )
        );
        preview = false;
      }

      // Validate stage owns configuration
      preview &= validateComponentConfigs(
        stageConf,
        stageDef.getConfigDefinitions(),
        stageDef.getConfigDefinitionsMap(),
        stageDef.getHideConfigs(),
        stageDef.hasPreconditions(),
        issueCreator
      );

      // Validate service definitions
      Set<String> expectedServices = stageDef.getServices().stream()
        .map(service -> service.getService().getName())
        .collect(Collectors.toSet());
      Set<String> configuredServices = stageConf.getServices().stream()
        .map(service -> service.getService().getName())
        .collect(Collectors.toSet());
      if(!expectedServices.equals(configuredServices)) {
        issues.add(issueCreator.create(
            stageConf.getInstanceName(),
            ValidationError.VALIDATION_0200,
            StringUtils.join(expectedServices, ","),
            StringUtils.join(configuredServices, ",")
        ));
        preview = false;
      } else {
        // Validate all services
        for (ServiceConfiguration serviceConf: stageConf.getServices()) {
          ServiceDefinition serviceDef = stageLibrary.getServiceDefinition(serviceConf.getService(), false);
          preview &= validateComponentConfigs(
            serviceConf,
            serviceDef.getConfigDefinitions(),
            serviceDef.getConfigDefinitionsMap(),
            Collections.emptySet(),
            false,
            issueCreator.forService(serviceConf.getService().getName())
          );
        }
      }
    }
    return preview;
  }

  private boolean validateComponentConfigs(
    UserConfigurable component,
    List<ConfigDefinition> configs,
    Map<String, ConfigDefinition> definitionMap,
    Set<String> hideConfigs,
    boolean validatePrecondition,
    IssueCreator issueCreator
  ) {
    boolean preview = true;

    // Validate user exposed configuration
    for (ConfigDefinition confDef : configs) {
      Config config = component.getConfig(confDef.getName());
      if (confDef.isRequired() && (config == null || isNullOrEmpty(confDef, config))) {
        preview &= validateRequiredField(confDef, component, issueCreator);
      }

      if(confDef.getType() == ConfigDef.Type.NUMBER && !isNullOrEmpty(confDef, config)) {
        preview &= validatedNumberConfig(config, confDef, component, issueCreator);
      }
    }

    for (Config conf : component.getConfiguration()) {
      ConfigDefinition confDef = definitionMap.get(conf.getName());
      preview &= validateConfigDefinition(confDef, hideConfigs, conf, component, definitionMap, null, issueCreator);
      if (confDef != null && validatePrecondition && confDef.getName().equals(StageConfigBean.STAGE_PRECONDITIONS_CONFIG)) {
        preview &= validatePreconditions(confDef, conf, issues, issueCreator);
      }
    }

    return preview;
  }

  boolean isNullOrEmpty(ConfigDefinition confDef, Config config) {
    boolean isNullOrEmptyString = false;
    if(config == null) {
      isNullOrEmptyString = true;
    } else if (config.getValue() == null) {
      isNullOrEmptyString = true;
    } else if (confDef.getType() == ConfigDef.Type.STRING) {
      if (((String) config.getValue()).isEmpty()) {
        isNullOrEmptyString = true;
      }
    } else if (confDef.getType() == ConfigDef.Type.LIST) {
      if (((List<?>) config.getValue()).isEmpty()) {
        isNullOrEmptyString = true;
      }
    } else if (confDef.getType() == ConfigDef.Type.MAP) {
      final Object value = config.getValue();
      if (value instanceof Collection) {
        if (((Collection<?>) value).isEmpty()) {
          isNullOrEmptyString = true;
        }
      } else if (value instanceof Map) {
        if (((Map<?,?>) value).isEmpty()) {
          isNullOrEmptyString = true;
        }
      } else {
        throw new IllegalStateException(String.format(
            "ConfigDefinition with name %s is type %s but config value class is instance of %s, with toString of %s",
            confDef.getName(),
            confDef.getType().name(),
            value.getClass().getName(),
            value.toString()
        ));
      }
    }
    return isNullOrEmptyString;
  }

  boolean validateRequiredField(
      ConfigDefinition confDef,
      UserConfigurable component,
      IssueCreator issueCreator
  ) {
    boolean preview = true;

    // If the config doesn't depend on anything or the config should be triggered, config is invalid
    if (confDef.getDependsOnMap().isEmpty() || configTriggered(component, confDef)) {
      issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(), ValidationError.VALIDATION_0007));
      preview = false;
    }
    return preview;
  }

  private boolean configTriggered(UserConfigurable component, ConfigDefinition confDef) {
    boolean triggered = true;

    for (Map.Entry<String, List<Object>> dependency : confDef.getDependsOnMap().entrySet()) {
      //At times the dependsOn config may be hidden [for ex. ToErrorKafkaTarget hides the dataFormat property].
      // In such a scenario component.getConfig(dependsOn) can be null. We need to guard against this.
      triggered &= (component.getConfig(dependency.getKey()) != null &&
          triggeredByContains(dependency.getValue(), component.getConfig(dependency.getKey()).getValue()));
    }
    return triggered;
  }

  boolean validatedNumberConfig(
      Config conf,
      ConfigDefinition confDef,
      UserConfigurable component,
      IssueCreator issueCreator
  ) {
    boolean preview = true;
    if (!configTriggered(component, confDef)) {
      return true;
    }
    if (conf.getValue() instanceof String && ((String)conf.getValue()).startsWith("${")
        && ((String)conf.getValue()).endsWith("}")) {
      // If value is EL, ignore max and min validation
      return true;
    }

    if (!(conf.getValue() instanceof Long || conf.getValue() instanceof Integer || conf.getValue() instanceof Double)) {
      issues.add(issueCreator.create(confDef.getGroup(),
          confDef.getName(), ValidationError.VALIDATION_0009,
          confDef.getType()));
      return false;
    }
    Double value = ((Number) conf.getValue()).doubleValue();
    if(value > confDef.getMax()) {
      issues.add(issueCreator.create(confDef.getGroup(),
          confDef.getName(), ValidationError.VALIDATION_0034, confDef.getName(), confDef.getMax()));
      preview = false;
    }
    if(value < confDef.getMin()) {
      issues.add(issueCreator.create(confDef.getGroup(),
          confDef.getName(), ValidationError.VALIDATION_0035, confDef.getName(), confDef.getMin()));
      preview = false;
    }
    return preview;
  }

  private boolean triggeredByContains(List<Object> triggeredBy, Object value) {
    boolean contains = false;
    for (Object object : triggeredBy) {
      if (String.valueOf(object).equals(String.valueOf(value))) {
        contains = true;
        break;
      }
    }
    return contains;
  }

  private static final Record PRECONDITION_RECORD = new RecordImpl("dummy", "dummy", null, null);

  @SuppressWarnings("unchecked")
  private boolean validatePreconditions(
      ConfigDefinition confDef,
      Config conf,
      Issues issues,
      IssueCreator issueCreator
  ) {
    boolean valid = true;
    if (conf.getValue() != null && conf.getValue() instanceof List) {
      List<String> list = (List<String>) conf.getValue();
      for (String precondition : list) {
        precondition = precondition.trim();
        if (!precondition.startsWith("${") || !precondition.endsWith("}")) {
          issues.add(
              issueCreator.create(
                  confDef.getGroup(),
                  confDef.getName(),
                  ValidationError.VALIDATION_0080,
                  precondition
              )
          );
          valid = false;
        } else {
          ELVariables elVars = new ELVariables();
          RecordEL.setRecordInContext(elVars, PRECONDITION_RECORD);
          try {
            ELEval elEval = new ELEvaluator(StageConfigBean.STAGE_PRECONDITIONS_CONFIG, false, constants,
                ConcreteELDefinitionExtractor.get(), confDef.getElDefs());
            elEval.eval(elVars, precondition, Boolean.class);
          } catch (ELEvalException ex) {
            issues.add(
                issueCreator.create(
                    confDef.getGroup(),
                    confDef.getName(),
                    ValidationError.VALIDATION_0081,
                    precondition,
                    ex.toString()
                )
            );
            valid = false;
          }
        }
      }
    }
    return valid;
  }

  private boolean validateConfigDefinition(
      ConfigDefinition confDef,
      Set<String> hideConfigs,
      Config conf,
      UserConfigurable stageConf,
      Map<String, ConfigDefinition> definitionMap,
      Map<String, Object> parentConf,
      IssueCreator issueCreator
  ) {
    //parentConf is applicable when validating complex fields.
    boolean preview = true;
    if (confDef == null && !hideConfigs.contains(conf.getName())) {
      // stage configuration defines an invalid configuration
      issues.add(
          issueCreator.create(
              ValidationError.VALIDATION_0008,
              conf.getName()
          )
      );
      return false;
    }
    boolean validateConfig = true;
    if (confDef != null) {
      for (Map.Entry<String, List<Object>> dependsOnEntry : confDef.getDependsOnMap().entrySet()){
        if (!dependsOnEntry.getValue().isEmpty()) {
          String dependsOn = dependsOnEntry.getKey();
          List<Object> triggeredBy = dependsOnEntry.getValue();
          Config dependsOnConfig = getConfig(stageConf.getConfiguration(), dependsOn);
          if (dependsOnConfig == null) {
            //complex field case?
            //look at the configurations in model definition
            if (parentConf != null && parentConf.containsKey(dependsOn)) {
              dependsOnConfig = new Config(dependsOn, parentConf.get(dependsOn));
            }
          }
          if (dependsOnConfig != null && dependsOnConfig.getValue() != null) {
            Object value = dependsOnConfig.getValue();
            validateConfig &= triggeredByContains(triggeredBy, value);
          }
        }
      }
      if (validateConfig && conf.getValue() != null && confDef.getModel() != null) {
        preview = validateModel(stageConf, definitionMap, hideConfigs, confDef, conf, issueCreator);
      }
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

  @SuppressWarnings("unchecked")
  private boolean validateModel(
      UserConfigurable stageConf,
      Map<String, ConfigDefinition> definitionMap,
      Set<String> hideConfigs,
      ConfigDefinition confDef,
      Config conf,
      IssueCreator issueCreator
  ) {
    boolean preview = true;
    switch (confDef.getModel().getModelType()) {
      case VALUE_CHOOSER:
        if (!(conf.getValue() instanceof String || conf.getValue().getClass().isEnum())) {
          // stage configuration must be a model
          issues.add(
              issueCreator.create(
                  confDef.getGroup(),
                  confDef.getName(),
                  ValidationError.VALIDATION_0009,
                  "String"
              )
          );
          preview = false;
        }
        break;
      case FIELD_SELECTOR_MULTI_VALUE:
        if (!(conf.getValue() instanceof List)) {
          // stage configuration must be a model
          issues.add(
              issueCreator.create(
                  confDef.getGroup(),
                  confDef.getName(),
                  ValidationError.VALIDATION_0009,
                  "List")
          );
          preview = false;
        } else {
          //validate all the field names for proper syntax
          List<String> fieldPaths = (List<String>) conf.getValue();
          for (String fieldPath : fieldPaths) {
            try {
              PathElement.parse(fieldPath, true);
            } catch (IllegalArgumentException e) {
              issues.add(
                  issueCreator.create(
                      confDef.getGroup(),
                      confDef.getName(),
                      ValidationError.VALIDATION_0033,
                      e.toString()
                  )
              );
              preview = false;
              break;
            }
          }
        }
        break;
      case LIST_BEAN:
        if (conf.getValue() != null) {
          //this can be a single HashMap or an array of hashMap
          Map<String, ConfigDefinition> configDefinitionsMap = new HashMap<>();
          for (ConfigDefinition c : confDef.getModel().getConfigDefinitions()) {
            configDefinitionsMap.put(c.getName(), c);
          }
          if (conf.getValue() instanceof List) {
            //list of hash maps
            List<Map<String, Object>> maps = (List<Map<String, Object>>) conf.getValue();
            for (Map<String, Object> map : maps) {
              preview &= validateComplexConfig(configDefinitionsMap, map, stageConf, definitionMap, hideConfigs, issueCreator);
            }
          } else if (conf.getValue() instanceof Map) {
            preview &= validateComplexConfig(
                configDefinitionsMap,
                (Map<String, Object>) conf.getValue(),
                stageConf,
                definitionMap,
                hideConfigs,
                issueCreator
            );
          }
        }
        break;
      case PREDICATE:
        if (!(conf.getValue() instanceof List)) {
          // stage configuration must be a model
          issues.add(
              issueCreator.create(
                  confDef.getGroup(),
                  confDef.getName(),
                  ValidationError.VALIDATION_0009,
                  "List<Map>"
              )
          );
          preview = false;
        } else {
          int count = 1;
          for (Object element : (List) conf.getValue()) {
            if (element instanceof Map) {
              Map map = (Map) element;
              if (!map.containsKey("outputLane")) {
                issues.add(
                    issueCreator.create(
                        confDef.getGroup(),
                        confDef.getName(),
                        ValidationError.VALIDATION_0020,
                        count,
                        "outputLane"
                    )
                );
                preview = false;
              } else {
                if (map.get("outputLane") == null) {
                  issues.add(
                      issueCreator.create(
                          confDef.getGroup(),
                          confDef.getName(),
                          ValidationError.VALIDATION_0021,
                          count,
                          "outputLane"
                      )
                  );
                  preview = false;
                } else {
                  if (!(map.get("outputLane") instanceof String)) {
                    issues.add(
                        issueCreator.create(
                            confDef.getGroup(),
                            confDef.getName(),
                            ValidationError.VALIDATION_0022,
                            count,
                            "outputLane"
                        )
                    );
                    preview = false;
                  } else if (((String) map.get("outputLane")).isEmpty()) {
                    issues.add(
                        issueCreator.create(
                            confDef.getGroup(),
                            confDef.getName(),
                            ValidationError.VALIDATION_0023,
                            count,
                            "outputLane"
                        )
                    );
                    preview = false;
                  }
                }
              }
              if (!map.containsKey("predicate")) {
                issues.add(
                    issueCreator.create(
                        confDef.getGroup(),
                        confDef.getName(),
                        ValidationError.VALIDATION_0020,
                        count,
                        "condition"
                    )
                );
                preview = false;
              } else {
                if (map.get("predicate") == null) {
                  issues.add(
                      issueCreator.create(
                          confDef.getGroup(),
                          confDef.getName(),
                          ValidationError.VALIDATION_0021,
                          count, "condition"
                      )
                  );
                  preview = false;
                } else {
                  if (!(map.get("predicate") instanceof String)) {
                    issues.add(
                        issueCreator.create(
                            confDef.getGroup(),
                            confDef.getName(),
                            ValidationError.VALIDATION_0022,
                            count,
                            "condition"
                        )
                    );
                    preview = false;
                  } else if (((String) map.get("predicate")).isEmpty()) {
                    issues.add(
                        issueCreator.create(
                            confDef.getGroup(),
                            confDef.getName(),
                            ValidationError.VALIDATION_0023,
                            count,
                            "condition"
                        )
                    );
                    preview = false;
                  }
                }
              }
            } else {
              issues.add(
                  issueCreator.create(
                      confDef.getGroup(),
                      confDef.getName(),
                      ValidationError.VALIDATION_0019,
                      count
                  )
              );
              preview = false;
            }
            count++;
          }
        }
        break;
      case FIELD_SELECTOR:
        // fall through
      case MULTI_VALUE_CHOOSER:
        break;
      default:
        throw new RuntimeException("Unknown model type: " + confDef.getModel().getModelType().name());
    }
    return preview;
  }

  private boolean validateComplexConfig(
      Map<String, ConfigDefinition> configDefinitionsMap,
      Map<String, Object> confvalue,
      UserConfigurable stageConf,
      Map<String, ConfigDefinition> definitionMap,
      Set<String> hideConfigs,
      IssueCreator issueCreator
  ) {
    boolean preview = true;
    for (Map.Entry<String, Object> entry : confvalue.entrySet()) {
      String configName = entry.getKey();
      Object value = entry.getValue();
      ConfigDefinition configDefinition = configDefinitionsMap.get(configName);
      Config config = new Config(configName, value);
      preview &= validateConfigDefinition(
          configDefinition,
          hideConfigs,
          config,
          stageConf,
          definitionMap,
          confvalue,
          issueCreator
      );
    }
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
        IssueCreator issueCreator = IssueCreator.getStage(stageConf.getStageName());
        issues.add(
          issueCreator.create(
              ValidationError.VALIDATION_0006,
              stageConf.getLibrary(),
              stageConf.getStageName(),
              stageConf.getStageVersion()
          )
        );
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
}
