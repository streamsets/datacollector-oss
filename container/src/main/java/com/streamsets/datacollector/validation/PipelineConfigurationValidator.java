/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.validation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineGroups;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.configupgrade.PipelineConfigurationUpgrader;
import com.streamsets.datacollector.creation.PipelineBean;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.el.JvmEL;
import com.streamsets.datacollector.el.RuntimeEL;
import com.streamsets.datacollector.record.PathElement;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.ElUtil;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.impl.TextUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringEL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PipelineConfigurationValidator {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigurationValidator.class);

  private final StageLibraryTask stageLibrary;
  private final String name;
  private PipelineConfiguration pipelineConfiguration;
  private final Issues issues;
  private final List<String> openLanes;
  private boolean validated;
  private boolean canPreview;
  private final Map<String, Object> constants;
  private PipelineBean pipelineBean;

  public PipelineConfigurationValidator(StageLibraryTask stageLibrary, String name,
      PipelineConfiguration pipelineConfiguration) {
    Preconditions.checkNotNull(stageLibrary, "stageLibrary cannot be null");
    Preconditions.checkNotNull(name, "name cannot be null");
    Preconditions.checkNotNull(pipelineConfiguration, "pipelineConfiguration cannot be null");
    this.stageLibrary = stageLibrary;
    this.name = name;
    this.pipelineConfiguration = pipelineConfiguration;
    issues = new Issues();
    openLanes = new ArrayList<>();
    this.constants = ElUtil.getConstants(pipelineConfiguration);
  }

  boolean sortStages() {
    boolean ok = true;
    List<StageConfiguration> original = new ArrayList<>(pipelineConfiguration.getStages());
    List<StageConfiguration> sorted = new ArrayList<>();
    Set<String> producedOutputs = new HashSet<>();
    while (ok && !original.isEmpty()) {
      int prior = sorted.size();
      Iterator<StageConfiguration> it = original.iterator();
      while (it.hasNext()) {
        StageConfiguration stage = it.next();
        if (producedOutputs.containsAll(stage.getInputLanes())) {
          producedOutputs.addAll(stage.getOutputLanes());
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
    pipelineConfiguration.setStages(sorted);
    return ok;
  }

  public PipelineConfiguration validate() {
    Preconditions.checkState(!validated, "Already validated");
    validated = true;
    LOG.trace("Pipeline '{}' starting validation", name);
    if (validSchemaVersion()) {
      canPreview = resolveLibraryAliases();
      canPreview &= upgradePipeline();
      canPreview &= sortStages();
      canPreview &= checkIfPipelineIsEmpty();
      canPreview &= loadPipelineConfig();
      canPreview &= validatePipelineMemoryConfiguration();
      canPreview &= validatePipelineConfiguration();
      canPreview &= validatePipelineLanes();
      canPreview &= validateErrorStage();
      canPreview &= validateStagesExecutionMode(pipelineConfiguration);

      if (LOG.isTraceEnabled() && issues.hasIssues()) {
        for (Issue issue : issues.getPipelineIssues()) {
          LOG.trace("Pipeline '{}', {}", name, issue);
        }
        for (Issue issue : issues.getIssues()) {
          LOG.trace("Pipeline '{}', {}", name, issue);
        }
      }
      LOG.debug("Pipeline '{}' validation. valid={}, canPreview={}, issuesCount={}", name, !issues.hasIssues(),
                canPreview, issues.getIssueCount());
    } else {
      LOG.debug("Pipeline '{}' validation. Unsupported pipeline schema '{}'", name,
                pipelineConfiguration.getSchemaVersion());
    }
    pipelineConfiguration.setValidation(this);
    return pipelineConfiguration;
  }

  boolean isLibraryAlias(String name) {
    return stageLibrary.getLibraryNameAliases().containsKey(name);
  }

  String resolveLibraryAlias(String name) {
    return stageLibrary.getLibraryNameAliases().get(name);
  }

  boolean resolveLibraryAliases() {
    if (pipelineConfiguration.getErrorStage() != null) {
      String name = pipelineConfiguration.getErrorStage().getLibrary();
      if (isLibraryAlias(name)) {
        pipelineConfiguration.getErrorStage().setLibrary(resolveLibraryAlias(name));
      }
    }
    for (StageConfiguration stageConf : pipelineConfiguration.getStages()) {
      String name = stageConf.getLibrary();
      if (isLibraryAlias(name)) {
        stageConf.setLibrary(resolveLibraryAlias(name));
      }
    }
    return true;
  }

  @VisibleForTesting
  PipelineConfigurationUpgrader getUpgrader() {
    return PipelineConfigurationUpgrader.get();
  }

  boolean upgradePipeline() {
    List<Issue> upgradeIssues = new ArrayList<>();
    PipelineConfiguration pConf = getUpgrader().upgradeIfNecessary(stageLibrary,
      pipelineConfiguration, upgradeIssues);
    if (pConf != null) {
      pipelineConfiguration = pConf;
    }
    issues.addAll(upgradeIssues);
    return upgradeIssues.isEmpty();
  }

  private boolean validateStageExecutionMode(StageConfiguration stageConf, ExecutionMode executionMode,
      List<Issue> issues, boolean errorStage) {
    boolean canPreview = true;
    IssueCreator issueCreator = IssueCreator.getStage(stageConf.getInstanceName());
    StageDefinition stageDef = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(), false);
    if (stageDef != null) {
      if (!stageDef.getExecutionModes().contains(executionMode)) {
        canPreview = false;
        if(errorStage) {
          issues.add(IssueCreator.getPipeline().create(PipelineGroups.BAD_RECORDS.name(), "badRecordsHandling",
            ValidationError.VALIDATION_0074, stageDef.getLabel(), stageDef.getLibraryLabel(), executionMode.getLabel()));
        } else {
          issues.add(issueCreator.create(ValidationError.VALIDATION_0071, stageDef.getLabel(),
            stageDef.getLibraryLabel(), executionMode.getLabel()));
        }
      }
    } else {
      canPreview = false;
      issues.add(issueCreator.create(ValidationError.VALIDATION_0006, stageConf.getLibrary(), stageConf.getStageName(),
                                     stageConf.getStageVersion()));
    }
    return canPreview;
  }

  private boolean validateStagesExecutionMode(PipelineConfiguration pipelineConf) {
    boolean canPreview = true;
    List<Issue> errors = new ArrayList<>();
    ExecutionMode pipelineExecutionMode = PipelineBeanCreator.get().getExecutionMode(pipelineConf, errors);
    if (errors.isEmpty()) {
      StageConfiguration errorStage = pipelineConf.getErrorStage();
      if (errorStage != null) {
        canPreview &= validateStageExecutionMode(errorStage, pipelineExecutionMode, errors, true);
      }
      for (StageConfiguration stageConf : pipelineConf.getStages()) {
        canPreview &= validateStageExecutionMode(stageConf, pipelineExecutionMode, errors, false);
      }
    } else {
      canPreview = false;
    }
    issues.addAll(errors);
    return canPreview;
  }

  private boolean loadPipelineConfig() {
    List<Issue> errors = new ArrayList<>();
    pipelineBean = PipelineBeanCreator.get().create(false, stageLibrary, pipelineConfiguration, errors);
    issues.addAll(errors);
    return errors.isEmpty();
  }

  private boolean validatePipelineMemoryConfiguration() {
    boolean canPreview = true;
    if (pipelineBean != null) {
      PipelineConfigBean config = pipelineBean.getConfig();
      if (config.memoryLimit > JvmEL.jvmMaxMemoryMB() * 0.85) {
        issues.add(
            IssueCreator.getPipeline().create("", "memoryLimit", ValidationError.VALIDATION_0063, config.memoryLimit,
                                              JvmEL.jvmMaxMemoryMB() * 0.85));
        canPreview = false;
      }
    }
    return canPreview;
  }

  //TODO eventually, this should trigger a schema upgrade
  public boolean validSchemaVersion() {
    if (pipelineConfiguration.getSchemaVersion() != PipelineStoreTask.SCHEMA_VERSION) {
      issues.add(
          IssueCreator.getPipeline().create(ValidationError.VALIDATION_0000, pipelineConfiguration.getSchemaVersion()));
      return false;
    } else {
      return true;
    }
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
    if (pipelineConfiguration.getStages().isEmpty()) {
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

  private boolean validateStageConfiguration(boolean shouldBeSource, StageConfiguration stageConf, boolean errorStage,
      IssueCreator issueCreator) {
    boolean preview = true;
    StageDefinition stageDef = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(),
                                                     false);
    if (stageDef == null) {
      // stage configuration refers to an undefined stage definition
      issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0006,
                                     stageConf.getLibrary(), stageConf.getStageName(), stageConf.getStageVersion()));
      preview = false;
    } else {
      if (shouldBeSource) {
        if (stageDef.getType() != StageType.SOURCE) {
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
        issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0016,
                                       TextUtils.VALID_NAME));
        preview = false;
      }
      for (String lane : stageConf.getInputLanes()) {
        if (!TextUtils.isValidName(lane)) {
          // stage instance input lane has an invalid name (it must match '[0-9A-Za-z_]+')
          issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0017, lane,
                                         TextUtils.VALID_NAME));
          preview = false;
        }
      }
      for (String lane : stageConf.getOutputLanes()) {
        if (!TextUtils.isValidName(lane)) {
          // stage instance output lane has an invalid name (it must match '[0-9A-Za-z_]+')
          issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0018, lane,
                                         TextUtils.VALID_NAME));
          preview = false;
        }
      }
      switch (stageDef.getType()) {
        case SOURCE:
          if (!stageConf.getInputLanes().isEmpty()) {
            // source stage cannot have input lanes
            issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0012,
                                           stageDef.getType(), stageConf.getInputLanes()));
            preview = false;
          }
          if (!stageDef.isVariableOutputStreams()) {
            // source stage must match the output stream defined in StageDef
            if (stageDef.getOutputStreams() != stageConf.getOutputLanes().size()) {
              issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0015,
                                             stageDef.getOutputStreams(), stageConf.getOutputLanes().size()));
            }
          } else if (stageConf.getOutputLanes().isEmpty()) {
            // source stage must have at least one output lane
            issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0032));
          }
          break;
        case PROCESSOR:
          if (stageConf.getInputLanes().isEmpty()) {
            // processor stage must have at least one input lane
            issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0014,
                                           stageDef.getType()));
            preview = false;
          }
          if (!stageDef.isVariableOutputStreams()) {
            // processor stage must match the output stream defined in StageDef
            if (stageDef.getOutputStreams() != stageConf.getOutputLanes().size()) {
              issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0015,
                                             stageDef.getOutputStreams(), stageConf.getOutputLanes().size()));
            }
          } else if (stageConf.getOutputLanes().isEmpty()) {
            // processor stage must have at least one output lane
            issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0032));
          }
          break;
        case TARGET:
          if (!errorStage && stageConf.getInputLanes().isEmpty()) {
            // target stage must have at least one input lane
            issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0014,
                                           stageDef.getType()));
            preview = false;
          }
          if (!stageConf.getOutputLanes().isEmpty()) {
            // target stage cannot have output lanes
            issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0013,
                                           stageDef.getType(), stageConf.getOutputLanes()));
            preview = false;
          }
          break;
      }
      for (ConfigDefinition confDef : stageDef.getConfigDefinitions()) {
        Config config = stageConf.getConfig(confDef.getName());
        if (confDef.isRequired() && (config == null || isNullOrEmpty(confDef, config))) {
          preview &= validateRequiredField(confDef, stageConf, issueCreator);
        }
      }
      for (Config conf : stageConf.getConfiguration()) {
        ConfigDefinition confDef = stageDef.getConfigDefinition(conf.getName());
        preview &= validateConfigDefinition(confDef, conf, stageConf, stageDef, null, issueCreator, true/*inject*/);
        if (stageDef.hasPreconditions() && confDef.getName().equals(ConfigDefinition.PRECONDITIONS)) {
          preview &= validatePreconditions(stageConf.getInstanceName(), confDef, conf, issues, issueCreator);
        }
      }
    }
    return preview;
  }

  private boolean isNullOrEmpty(ConfigDefinition confDef, Config config) {
    boolean isNullOrEmptyString = false;
    if(config.getValue() == null) {
      isNullOrEmptyString = true;
    } else if (confDef.getType() == ConfigDef.Type.STRING) {
      if(((String) config.getValue()).isEmpty()) {
        isNullOrEmptyString = true;
      }
    } else if (confDef.getType() == ConfigDef.Type.LIST) {
      if(((List<?>) config.getValue()).isEmpty()) {
        isNullOrEmptyString = true;
      }
    } else if (confDef.getType() == ConfigDef.Type.MAP) {
      if(((Map<?,?>) config.getValue()).isEmpty()) {
        isNullOrEmptyString = true;
      }
    }
    return isNullOrEmptyString;
  }

  private boolean validateRequiredField(ConfigDefinition confDef, StageConfiguration stageConf,
                                        IssueCreator issueCreator) {
    boolean preview = true;
    String dependsOn = confDef.getDependsOn();
    List<Object> triggeredBy = confDef.getTriggeredByValues();
    // If the config doesn't depend on anything or the config should be triggered, config is invalid
    if (dependsOn == null || dependsOn.isEmpty() ||
      /*At times the dependsOn config may be hidden [for ex. ToErrorKafkaTarget hides the dataFormat property].
      * In such a scenario stageConf.getConfig(dependsOn) can be null. We need to guard against this.*/
      (stageConf.getConfig(dependsOn) != null &&
        triggeredByContains(triggeredBy, stageConf.getConfig(dependsOn).getValue()))) {
      issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(), ValidationError.VALIDATION_0007));
      preview = false;
    }
    return preview;
  }

  private boolean triggeredByContains(List<Object> triggeredBy, Object value) {
    boolean contains = false;
    for(Object object : triggeredBy) {
      if(String.valueOf(object).equals(String.valueOf(value))) {
        contains = true;
        break;
      }
    }
    return contains;
  }

  private static final Record PRECONDITION_RECORD = new RecordImpl("dummy", "dummy", null, null);

  boolean validatePreconditions(String instanceName, ConfigDefinition confDef, Config conf, Issues issues,
      IssueCreator issueCreator) {
    boolean valid = true;
    if (conf.getValue() != null && conf.getValue() instanceof List) {
      List<String> list = (List<String>) conf.getValue();
      for (String precondition : list) {
        precondition = precondition.trim();
        if (!precondition.startsWith("${") || !precondition.endsWith("}")) {
          issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(),
                                         ValidationError.VALIDATION_0080, precondition));
          valid = false;
        } else {
          ELVariables elVars = new ELVariables();
          RecordEL.setRecordInContext(elVars, PRECONDITION_RECORD);
          try {
            ELEval elEval = new ELEvaluator(ConfigDefinition.PRECONDITIONS, constants,
                                            RecordEL.class, StringEL.class, RuntimeEL.class);
            elEval.eval(elVars, precondition, Boolean.class);
          } catch (ELEvalException ex) {
            issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(),
                                           ValidationError.VALIDATION_0081, precondition, ex.toString()));
            valid = false;
          }
        }
      }
    }
    return valid;
  }

  private boolean validateConfigDefinition(ConfigDefinition confDef, Config conf,
                                           StageConfiguration stageConf, StageDefinition stageDef,
                                           Map<String, Object> parentConf, IssueCreator issueCreator,
                                           boolean inject) {
    //parentConf is applicable when validating complex fields.
    boolean preview = true;
    if (confDef == null) {
      // stage configuration defines an invalid configuration
      issues.add(issueCreator.create(stageConf.getInstanceName(), null, conf.getName(),
                                     ValidationError.VALIDATION_0008));
      return false;
    }
    boolean validateConfig = true;
    if (confDef.getDependsOn() != null &&
      (confDef.getTriggeredByValues() != null && confDef.getTriggeredByValues().size() > 0)) {
      String dependsOn = confDef.getDependsOn();
      List<Object> triggeredBy = confDef.getTriggeredByValues();
      Config dependsOnConfig = getConfig(stageConf.getConfiguration(), dependsOn);
      if(dependsOnConfig == null) {
        //complex field case?
        //look at the configurations in model definition
        if(parentConf != null && parentConf.containsKey(dependsOn)) {
          dependsOnConfig = new Config(dependsOn, parentConf.get(dependsOn));
        }
      }
      if (dependsOnConfig != null && dependsOnConfig.getValue() != null) {
        validateConfig = false;
        Object value = dependsOnConfig.getValue();
        for (Object trigger : triggeredBy) {
          validateConfig |= String.valueOf(value).equals(String.valueOf(trigger));
        }
      }
    }
    if(conf.getValue() != null && confDef.getModel() != null) {
      preview &= validateModel(stageConf, stageDef, confDef, conf, issueCreator);
    }
    return preview;
  }

  @VisibleForTesting
  boolean validatePipelineConfiguration() {
    boolean preview = true;
    Set<String> stageNames = new HashSet<>();
    boolean shouldBeSource = true;
    for (StageConfiguration stageConf : pipelineConfiguration.getStages()) {
      if (stageNames.contains(stageConf.getInstanceName())) {
        // duplicate stage instance name in the pipeline
        issues.add(IssueCreator.getStage(stageConf.getInstanceName()).create(stageConf.getInstanceName(),
                                                                             ValidationError.VALIDATION_0005));
        preview = false;
      }
      preview &= validateStageConfiguration(shouldBeSource, stageConf, false,
                                            IssueCreator.getStage(stageConf.getInstanceName()));
      stageNames.add(stageConf.getInstanceName());
      shouldBeSource = false;
    }
    return preview;
  }

  private boolean validateModel(StageConfiguration stageConf, StageDefinition stageDef, ConfigDefinition confDef, Config conf,
      IssueCreator issueCreator) {
    String instanceName = stageConf.getInstanceName();
    boolean preview = true;
    switch (confDef.getModel().getModelType()) {
      case VALUE_CHOOSER:
        if(!(conf.getValue() instanceof String || conf.getValue().getClass().isEnum()) ) {
          // stage configuration must be a model
          issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(),
                                         ValidationError.VALIDATION_0009, "String"));
          preview = false;
        }
        break;
      case FIELD_SELECTOR_MULTI_VALUED:
        if(!(conf.getValue() instanceof List)) {
          // stage configuration must be a model
          issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(),
                                         ValidationError.VALIDATION_0009, "List"));
          preview = false;
        } else {
          //validate all the field names for proper syntax
          List<String> fieldPaths = (List<String>) conf.getValue();
          for(String fieldPath : fieldPaths) {
            try {
              PathElement.parse(fieldPath, false);
            } catch (IllegalArgumentException e) {
              issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(), ValidationError.VALIDATION_0033,
                e.toString()));
              preview = false;
              break;
            }
          }
        }
        break;
      case COMPLEX_FIELD:
        if(conf.getValue() != null) {
          //this can be a single HashMap or an array of hashMap
          Map<String, ConfigDefinition> configDefinitionsMap = new HashMap<>();
          for (ConfigDefinition c : confDef.getModel().getConfigDefinitions()) {
            configDefinitionsMap.put(c.getName(), c);
          }
          if (conf.getValue() instanceof List) {
            //list of hash maps
            List<Map<String, Object>> maps = (List<Map<String, Object>>) conf.getValue();
            for (Map<String, Object> map : maps) {
              preview &= validateComplexConfig(configDefinitionsMap, map, stageConf, stageDef, issueCreator);
            }
          } else if (conf.getValue() instanceof Map) {
            preview &= validateComplexConfig(configDefinitionsMap, (Map<String, Object>) conf.getValue(), stageConf,
              stageDef, issueCreator);
          }
        }
        break;
      case FIELD_VALUE_CHOOSER:
        if(!(conf.getValue() instanceof Map)) {
          // stage configuration must be a model
          issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(),
                                         ValidationError.VALIDATION_0009, "Map"));
          preview = false;
        }
        break;
      case LANE_PREDICATE_MAPPING:
        if(!(conf.getValue() instanceof List)) {
          // stage configuration must be a model
          issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(),
                                         ValidationError.VALIDATION_0009, "List<Map>"));
          preview = false;
        } else {
          int count = 1;
          for (Object element : (List) conf.getValue()) {
            if (element instanceof Map) {
              Map map = (Map)element;
              if (!map.containsKey("outputLane")) {
                issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(),
                                               ValidationError.VALIDATION_0020, count, "outputLane"));
                preview = false;
              } else {
                if (map.get("outputLane") == null) {
                  issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(),
                                                 ValidationError.VALIDATION_0021, count, "outputLane"));
                  preview = false;
                } else {
                  if (!(map.get("outputLane") instanceof String)) {
                    issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(),
                                                   ValidationError.VALIDATION_0022, count, "outputLane"));
                    preview = false;
                  } else if (((String)map.get("outputLane")).isEmpty()) {
                    issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(),
                                                   ValidationError.VALIDATION_0023, count, "outputLane"));
                    preview = false;
                  }
                }
              }
              if (!map.containsKey("predicate")) {
                issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(),
                                               ValidationError.VALIDATION_0020, count, "condition"));
                preview = false;
              } else {
                if (map.get("predicate") == null) {
                  issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(),
                                                 ValidationError.VALIDATION_0021, count, "condition"));
                  preview = false;
                } else {
                  if (!(map.get("predicate") instanceof String)) {
                    issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(),
                                                   ValidationError.VALIDATION_0022, count, "condition"));
                    preview = false;
                  } else if (((String)map.get("predicate")).isEmpty()) {
                    issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(),
                                                   ValidationError.VALIDATION_0023, count, "condition"));
                    preview = false;
                  }
                }
              }
            } else {
              issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(),
                                             ValidationError.VALIDATION_0019, count));
              preview = false;
            }
            count++;
          }
         }
        break;
    }
    return preview;
  }

  private boolean validateComplexConfig(Map<String, ConfigDefinition> configDefinitionsMap,
                                        Map<String, Object> confvalue , StageConfiguration stageConf,
                                        StageDefinition stageDef, IssueCreator issueCreator) {
    boolean preview = true;
    for(Map.Entry<String, Object> entry : confvalue.entrySet()) {
      String configName = entry.getKey();
      Object value = entry.getValue();
      ConfigDefinition configDefinition = configDefinitionsMap.get(configName);
      Config config = new Config(configName, value);
      preview &= validateConfigDefinition(configDefinition, config, stageConf, stageDef, confvalue,
        issueCreator, false /*do not inject*/);
    }
    return preview;
  }


  @VisibleForTesting
  boolean validatePipelineLanes() {
    boolean preview = true;
    List<StageConfiguration> stagesConf = pipelineConfiguration.getStages();
    for (int i = 0; i < stagesConf.size(); i++) {
      StageConfiguration stageConf = stagesConf.get(i);
      Set<String> openOutputs = new LinkedHashSet<>(stageConf.getOutputLanes());
      for (int j = i + 1; j < stagesConf.size(); j++) {
        StageConfiguration downStreamStageConf = stagesConf.get(j);
        Set<String> duplicateOutputs = Sets.intersection(new HashSet<>(stageConf.getOutputLanes()),
                                                         new HashSet<>(downStreamStageConf.getOutputLanes()));
        if (!duplicateOutputs.isEmpty()) {
          // there is more than one stage defining the same output lane
          issues.add(IssueCreator.getPipeline().create(downStreamStageConf.getInstanceName(),
                                                       ValidationError.VALIDATION_0010,
                                                       duplicateOutputs, stageConf.getInstanceName()));
          preview = false;
        }

        openOutputs.removeAll(downStreamStageConf.getInputLanes());
      }
      if (!openOutputs.isEmpty()) {
        openLanes.addAll(openOutputs);
        // the stage has open output lanes
        Issue issue = IssueCreator.getStage(stageConf.getInstanceName()).create(ValidationError.VALIDATION_0011);
        issue.setAdditionalInfo("openStreams", openOutputs);
        issues.add(issue);
      }
    }
    return preview;
  }

  @VisibleForTesting
  boolean validateErrorStage() {
    boolean preview = false;
    StageConfiguration errorStage = pipelineConfiguration.getErrorStage();
    if (errorStage != null) {
      IssueCreator errorStageCreator = IssueCreator.getStage(errorStage.getInstanceName());
      preview = validateStageConfiguration(false, errorStage, true, errorStageCreator);
    }
    return preview;
  }

}
