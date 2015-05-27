/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.impl.TextUtils;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.ModelType;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.PipelineDefConfigs;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.ElUtil;
import com.streamsets.pipeline.util.ValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.io.File;

public class PipelineConfigurationValidator {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigurationValidator.class);

  private final StageLibraryTask stageLibrary;
  private final String name;
  private final PipelineConfiguration pipelineConfiguration;
  private final Issues issues;
  private final List<String> openLanes;
  private boolean validated;
  private boolean canPreview;
  private final boolean evaluateElExpressions;
  private final Map<String, Object> constants;

  public PipelineConfigurationValidator(StageLibraryTask stageLibrary, String name,
                                        PipelineConfiguration pipelineConfiguration, boolean evaluateElExpressions) {
    Preconditions.checkNotNull(stageLibrary, "stageLibrary cannot be null");
    Preconditions.checkNotNull(name, "name cannot be null");
    Preconditions.checkNotNull(pipelineConfiguration, "pipelineConfiguration cannot be null");
    this.stageLibrary = stageLibrary;
    this.name = name;
    this.pipelineConfiguration = pipelineConfiguration;
    issues = new Issues();
    openLanes = new ArrayList<>();
    this.evaluateElExpressions = evaluateElExpressions;
    this.constants = ElUtil.getConstants(pipelineConfiguration);
  }

  public PipelineConfigurationValidator(StageLibraryTask stageLibrary, String name,
                                        PipelineConfiguration pipelineConfiguration) {
    this(stageLibrary, name, pipelineConfiguration, true /*evaluate expressions*/);
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
        issues.addP(new Issue(ValidationError.VALIDATION_0002, names));
        ok = false;
      }
    }
    sorted.addAll(original);
    pipelineConfiguration.setStages(sorted);
    return ok;
  }

  public boolean validate() {
    Preconditions.checkState(!validated, "Already validated");
    validated = true;
    LOG.trace("Pipeline '{}' starting validation", name);
    if (validSchemaVersion()) {
      canPreview = sortStages();
      canPreview &= checkIfPipelineIsEmpty();
      canPreview &= validatePipelineMemoryConfiguration();
      canPreview &= validatePipelineConfiguration(StageIssueCreator.getStageCreator());
      canPreview &= validatePipelineLanes(StageIssueCreator.getStageCreator());
      canPreview &= validateErrorStage();
      canPreview &= validateStagesExecutionMode(StageIssueCreator.getStageCreator(), pipelineConfiguration.getStages());
      canPreview &= validateClusterModeConfig();

      if (LOG.isTraceEnabled() && issues.hasIssues()) {
        for (Issue issue : issues.getPipelineIssues()) {
          LOG.trace("Pipeline '{}', {}", name, issue);
        }
        for (List<StageIssue> stageIssues : issues.getStageIssues().values()) {
          for (StageIssue stageIssue : stageIssues) {
            LOG.trace("Pipeline '{}', {}", name, stageIssue);
          }
        }
      }
      LOG.debug("Pipeline '{}' validation. valid={}, canPreview={}, issuesCount={}", name, !issues.hasIssues(),
                canPreview, issues.getIssueCount());
    } else {
      LOG.debug("Pipeline '{}' validation. Unsupported pipeline schema '{}'", name,
                pipelineConfiguration.getSchemaVersion());
    }
    return !issues.hasIssues();
  }
  private boolean validateClusterModeConfig() {
    ConfigConfiguration executionConfigMode = pipelineConfiguration.getConfiguration(PipelineDefConfigs.
      EXECUTION_MODE_CONFIG);
    ExecutionMode executionMode = (executionConfigMode == null) ? ExecutionMode.STANDALONE
      : ExecutionMode.valueOf((String) executionConfigMode.getValue());
    ConfigConfiguration kerberosAuth = pipelineConfiguration.getConfiguration(PipelineDefConfigs.
      CLUSTER_KERBEROS_AUTH_CONFIG);
    if (executionMode == ExecutionMode.CLUSTER && kerberosAuth != null && (Boolean)kerberosAuth.getValue()
      && !Boolean.getBoolean(RuntimeInfo.TRANSIENT_ENVIRONMENT)) {
      ConfigConfiguration kerberosPrinc = pipelineConfiguration.getConfiguration(PipelineDefConfigs.
        CLUSTER_KERBEROS_PRINCIPAL_CONFIG);
      ConfigConfiguration kerberosKeytab = pipelineConfiguration.getConfiguration(PipelineDefConfigs.
        CLUSTER_KERBEROS_KEYTAB_CONFIG);
      if (Strings.nullToEmpty((String)kerberosPrinc.getValue()).trim().isEmpty()) {
        issues.addP(new Issue(kerberosPrinc.getName(), "CLUSTER", ValidationError.VALIDATION_0033,
          "Kerberos Principal is empty"));
      }
      if (Strings.nullToEmpty((String)kerberosKeytab.getValue()).trim().isEmpty()) {
        issues.addP(new Issue(kerberosKeytab.getName(), "CLUSTER", ValidationError.VALIDATION_0033,
          "Kerberos Keytab is empty"));
      } else {
        File keyTab = new File((String)kerberosKeytab.getValue());
        if (!keyTab.isFile() || !keyTab.canRead()) {
          issues.addP(new Issue(kerberosKeytab.getName(), "CLUSTER", ValidationError.VALIDATION_0033,
            "Kerberos Keytab file does not exist or cannot be read"));
        }
      }
    }
    return true;
  }
  private boolean validateStagesExecutionMode(StageIssueCreator issueCreator, List<StageConfiguration> stageConfigs) {
    boolean canPreview = true;
    ConfigConfiguration conf = pipelineConfiguration.getConfiguration(PipelineDefConfigs.EXECUTION_MODE_CONFIG);
    ExecutionMode executionMode = (conf == null) ? ExecutionMode.STANDALONE
                                                 : ExecutionMode.valueOf((String) conf.getValue());
    for (StageConfiguration stageConf : stageConfigs) {
      StageDefinition stageDef = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(),
                                                       stageConf.getStageVersion());
      if (stageDef != null) {
        if (!stageDef.getExecutionModes().contains(executionMode)) {
          issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0071,
            executionMode));
          canPreview = false;
        } else if (!stageDef.getLibraryExecutionModes().contains(executionMode)) {
          String type;
          if (stageDef.getType() == StageType.SOURCE) {
            type = "Origin";
          } else if (stageDef.getType() == StageType.TARGET) {
            type = "Destination";
          } else {
            type = "Processor";
          }
          issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0074,
            stageDef.getLibraryLabel(), executionMode.getLabel(), type));
          canPreview = false;
        }
      } else {
        issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0006,
          stageConf.getLibrary(), stageConf.getStageName(), stageConf.getStageVersion()));
      }
    }
    return canPreview;
  }

  private boolean validatePipelineMemoryConfiguration() {
    List<ConfigConfiguration> configs = pipelineConfiguration.getConfiguration();
    if (configs != null) {
      for (ConfigConfiguration config : configs) {
        if (PipelineDefConfigs.MEMORY_LIMIT_CONFIG.equals(config.getName())) {
          try {
            //Memory limit configuration expects a long value.
            //However the user could provide an El expression or refer to this value using an EL constant or
            // ${runtime:conf("<memLimitVar>")}
            //The EL expression should not be validated unless the user clicks on the validate button, tries to
            //preview or tun the pipeline.
            long memoryLimit = Integer.MIN_VALUE;
            String memoryLimitString = String.valueOf(config.getValue());
            if(ElUtil.isElString(memoryLimitString)) {
              //validate only if required. This will be true during explicit validate , preview and run
              if(evaluateElExpressions) {
                try {
                  memoryLimit = ValidationUtil.evaluateMemoryLimit(memoryLimitString, constants);
                } catch (ELEvalException e) {
                  issues.addP(new Issue(config.getName(), "", ValidationError.VALIDATION_0064, e.getMessage(), e));
                  return false;
                }
              }
            } else if (!ElUtil.isElString(memoryLimitString)) {
              memoryLimit = Long.parseLong(memoryLimitString);
            }

            if(memoryLimit != Integer.MIN_VALUE) {
              if (memoryLimit > PipelineDefConfigs.MEMORY_LIMIT_MAX) {
                issues.addP(new Issue(config.getName(), "", ValidationError.VALIDATION_0063, memoryLimit,
                  "above the maximum", PipelineDefConfigs.MEMORY_LIMIT_MAX));
                return false;
              }
            }
          } catch (NumberFormatException e) {
            issues.addP(new Issue(config.getName(), "", ValidationError.VALIDATION_0062, config.getValue()));
            return false;
          }
        }
      }
    }
    return true;
  }

  //TODO eventually, this should trigger a schema upgrade
  public boolean validSchemaVersion() {
    if (pipelineConfiguration.getSchemaVersion() != PipelineStoreTask.SCHEMA_VERSION) {
      issues.addP(new Issue(ValidationError.VALIDATION_0000, pipelineConfiguration.getSchemaVersion()));
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
      issues.addP(new Issue(ValidationError.VALIDATION_0001));
      preview = false;
    }
    return preview;
  }

  private ConfigConfiguration getConfig(List<ConfigConfiguration> configs, String name) {
    for (ConfigConfiguration config : configs) {
      if (config.getName().equals(name)) {
        return config;
      }
    }
    return null;
  }

  private boolean validateStageConfiguration(boolean shouldBeSource, StageConfiguration stageConf, boolean errorStage,
      StageIssueCreator issueCreator) {
    boolean preview = true;
    StageDefinition stageDef = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(),
                                                     stageConf.getStageVersion());
    if (stageDef == null) {
      // stage configuration refers to an undefined stage definition
      issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0006,
                                             stageConf.getLibrary(), stageConf.getStageName(), stageConf.getStageVersion()));
      preview = false;
    } else {
      if (shouldBeSource) {
        if (stageDef.getType() != StageType.SOURCE) {
          // first stage must be a Source
          issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0003));
          preview = false;
        }
      } else {
        if (stageDef.getType() == StageType.SOURCE) {
          // no stage other than first stage can be a Source
          issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0004));
          preview = false;
        }
      }
      if (!stageConf.isSystemGenerated() && !TextUtils.isValidName(stageConf.getInstanceName())) {
        // stage instance name has an invalid name (it must match '[0-9A-Za-z_]+')
        issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0016,
                                               TextUtils.VALID_NAME));
        preview = false;
      }
      for (String lane : stageConf.getInputLanes()) {
        if (!TextUtils.isValidName(lane)) {
          // stage instance input lane has an invalid name (it must match '[0-9A-Za-z_]+')
          issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0017, lane,
                                                 TextUtils.VALID_NAME));
          preview = false;
        }
      }
      for (String lane : stageConf.getOutputLanes()) {
        if (!TextUtils.isValidName(lane)) {
          // stage instance output lane has an invalid name (it must match '[0-9A-Za-z_]+')
          issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0018, lane,
                                                 TextUtils.VALID_NAME));
          preview = false;
        }
      }
      switch (stageDef.getType()) {
        case SOURCE:
          if (!stageConf.getInputLanes().isEmpty()) {
            // source stage cannot have input lanes
            issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0012,
                                                   stageDef.getType(), stageConf.getInputLanes()));
            preview = false;
          }
          if (!stageDef.isVariableOutputStreams()) {
            // source stage must match the output stream defined in StageDef
            if (stageDef.getOutputStreams() != stageConf.getOutputLanes().size()) {
              issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0015,
                                                     stageDef.getOutputStreams(), stageConf.getOutputLanes().size()));
            }
          } else if (stageConf.getOutputLanes().isEmpty()) {
            // source stage must have at least one output lane
            issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0032));
          }
          break;
        case PROCESSOR:
          if (stageConf.getInputLanes().isEmpty()) {
            // processor stage must have at least one input lane
            issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0014,
                                                   stageDef.getType()));
            preview = false;
          }
          if (!stageDef.isVariableOutputStreams()) {
            // processor stage must match the output stream defined in StageDef
            if (stageDef.getOutputStreams() != stageConf.getOutputLanes().size()) {
              issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0015,
                                                     stageDef.getOutputStreams(), stageConf.getOutputLanes().size()));
            }
          } else if (stageConf.getOutputLanes().isEmpty()) {
            // processor stage must have at least one output lane
            issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0032));
          }
          break;
        case TARGET:
          if (!errorStage && stageConf.getInputLanes().isEmpty()) {
            // target stage must have at least one input lane
            issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0014,
                                                   stageDef.getType()));
            preview = false;
          }
          if (!stageConf.getOutputLanes().isEmpty()) {
            // target stage cannot have output lanes
            issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0013,
                                                   stageDef.getType(), stageConf.getOutputLanes()));
            preview = false;
          }
          break;
      }
      for (ConfigDefinition confDef : stageDef.getConfigDefinitions()) {
        if (stageConf.getConfig(confDef.getName()) == null && confDef.isRequired()) {
          // stage configuration does not have a configuration that is required
          issues.add(issueCreator.createConfigIssue(stageConf.getInstanceName(), confDef.getGroup(), confDef.getName(),
                                                  ValidationError.VALIDATION_0007));
          preview = false;
        }
      }
      for (ConfigConfiguration conf : stageConf.getConfiguration()) {
        ConfigDefinition confDef = stageDef.getConfigDefinition(conf.getName());
        preview &= validateConfigDefinition(confDef, conf, stageConf, stageDef, null, issueCreator, true/*inject*/);
      }
    }
    return preview;
  }

  private boolean validateConfigDefinition(ConfigDefinition confDef, ConfigConfiguration conf,
                                           StageConfiguration stageConf, StageDefinition stageDef,
                                           Map<String, Object> parentConf, StageIssueCreator issueCreator,
                                           boolean inject) {
    //parentConf is applicable when validating complex fields.
    boolean preview = true;
    if (confDef == null) {
      // stage configuration defines an invalid configuration
      issues.add(issueCreator.createConfigIssue(stageConf.getInstanceName(), null, conf.getName(),
        ValidationError.VALIDATION_0008));
      return false;
    } else if (conf.getValue() == null && confDef.isRequired()) {
      // stage configuration has a NULL value for a configuration that requires a value
      String dependsOn = confDef.getDependsOn();
      List<Object> triggeredBy = confDef.getTriggeredByValues();
      // If the config doesn't depend on anything or the config should be triggered, config is invalid
      if (dependsOn == null || dependsOn.isEmpty() ||
        (triggeredBy.contains(stageConf.getConfig(dependsOn).getValue()))) {
        issues.add(issueCreator.createConfigIssue(stageConf.getInstanceName(), confDef.getGroup(), confDef.getName(),
          ValidationError.VALIDATION_0007));
        preview = false;
      }
    }
    boolean validateConfig = true;
    if (confDef.getDependsOn() != null &&
      (confDef.getTriggeredByValues() != null && confDef.getTriggeredByValues().size() > 0)) {
      String dependsOn = confDef.getDependsOn();
      List<Object> triggeredBy = confDef.getTriggeredByValues();
      ConfigConfiguration dependsOnConfig = getConfig(stageConf.getConfiguration(), dependsOn);
      if(dependsOnConfig == null) {
        //complex field case?
        //look at the configurations in model definition
        if(parentConf != null && parentConf.containsKey(dependsOn)) {
          dependsOnConfig = new ConfigConfiguration(dependsOn, parentConf.get(dependsOn));
        }
      }
      if (dependsOnConfig != null && dependsOnConfig.getValue() != null) {
        validateConfig = false;
        Object value = dependsOnConfig.getValue();
        for (Object trigger : triggeredBy) {
          validateConfig |= value.equals(trigger);
        }
      }
    }
    if (validateConfig && conf.getValue() != null) {
      //inject value into config configuration
      if(inject && evaluateElExpressions) {
        conf = injectConfiguration(conf, confDef, stageDef, stageConf, pipelineConfiguration, issueCreator);
      }
      if(conf == null) {
        return false;
      }
      switch (confDef.getType()) {
        case BOOLEAN:
          if (!(conf.getValue() instanceof Boolean)) {
            issues.add(issueCreator.createConfigIssue(stageConf.getInstanceName(), confDef.getGroup(),
              confDef.getName(), ValidationError.VALIDATION_0009,
              confDef.getType()));
            preview = false;
          }
          break;
        case NUMBER:
          if (!(conf.getValue() instanceof Long || conf.getValue() instanceof Integer)) {
            issues.add(issueCreator.createConfigIssue(stageConf.getInstanceName(), confDef.getGroup(),
              confDef.getName(), ValidationError.VALIDATION_0009,
              confDef.getType()));
            preview = false;
            break;
          }
          Long value = ((Number) conf.getValue()).longValue();
          if(value > confDef.getMax()) {
            issues.add(issueCreator.createConfigIssue(stageConf.getInstanceName(), confDef.getGroup(),
              confDef.getName(), ValidationError.VALIDATION_0034, confDef.getName(), confDef.getMax()));
            preview = false;
          }
          if(value < confDef.getMin()) {
            issues.add(issueCreator.createConfigIssue(stageConf.getInstanceName(), confDef.getGroup(),
              confDef.getName(), ValidationError.VALIDATION_0035, confDef.getName(), confDef.getMin()));
            preview = false;
          }
          break;
        case STRING:
          if (!(conf.getValue() instanceof String)) {
            issues.add(issueCreator.createConfigIssue(stageConf.getInstanceName(), confDef.getGroup(),
              confDef.getName(), ValidationError.VALIDATION_0009,
              confDef.getType()));
            preview = false;
          }
          break;
        case CHARACTER:
          if(conf.getValue() instanceof Character) {

          } else if (conf.getValue() instanceof String) {
            if (((String)conf.getValue()).length() > 1) {
              issues.add(issueCreator.createConfigIssue(stageConf.getInstanceName(), confDef.getGroup(),
                confDef.getName(), ValidationError.VALIDATION_0031,
                conf.getValue()));
              preview = false;
            }
          } else {
            issues.add(issueCreator.createConfigIssue(stageConf.getInstanceName(), confDef.getGroup(),
              confDef.getName(), ValidationError.VALIDATION_0009,
              confDef.getType()));
            preview = false;
          }
          break;
        case MAP:
          if (conf.getValue() instanceof List) {
            int count = 0;
            for (Object element : (List) conf.getValue()) {
              if (element == null) {
                issues.add(issueCreator.createConfigIssue(stageConf.getInstanceName(), confDef.getGroup(),
                  confDef.getName(), ValidationError.VALIDATION_0024,
                  count));
                preview = false;
              } else if (element instanceof Map) {
                Map map = (Map) element;
                if (!map.containsKey("key") || !map.containsKey("value")) {
                  issues.add(issueCreator.createConfigIssue(stageConf.getInstanceName(), confDef.getGroup(),
                    confDef.getName(), ValidationError.VALIDATION_0025,
                    count));
                  preview = false;
                }
              } else {
                issues.add(issueCreator.createConfigIssue(stageConf.getInstanceName(), confDef.getGroup(),
                  confDef.getName(), ValidationError.VALIDATION_0026, count,
                  element.getClass().getSimpleName()));
                preview = false;
              }
              count++;
            }
          } else if (!(conf.getValue() instanceof Map)) {
            issues.add(issueCreator.createConfigIssue(stageConf.getInstanceName(), confDef.getGroup(),
              confDef.getName(), ValidationError.VALIDATION_0009,
              confDef.getType()));
            preview = false;
          }
          break;
        case LIST:
          if (!(conf.getValue() instanceof List)) {
            issues.add(issueCreator.createConfigIssue(stageConf.getInstanceName(), confDef.getGroup(),
              confDef.getName(), ValidationError.VALIDATION_0009,
              confDef.getType()));
            preview = false;
          }
          break;
        case MODEL:
          preview &= validateModel(stageConf, stageDef, confDef, conf, issueCreator);
          break;
      }
    }
    return preview;
  }

  private ConfigConfiguration injectConfiguration(ConfigConfiguration conf, ConfigDefinition confDef,
                                                        StageDefinition stageDef, StageConfiguration stageConf,
                                                        PipelineConfiguration pipelineConf, StageIssueCreator issueCreator) {
    switch (confDef.getName()) {
      case ConfigDefinition.REQUIRED_FIELDS:
      case ConfigDefinition.ON_RECORD_ERROR:
        return conf;
      default:
        try {
          //check if the field is an enum and convert the value to enum if so
          Class klass = stageDef.getStageClassLoader().loadClass(stageDef.getClassName());
          Field var = klass.getField(confDef.getFieldName());
          if (confDef.getModel() != null && confDef.getModel().getModelType() == ModelType.COMPLEX_FIELD) {
            return injectComplexConfiguration(var, conf, confDef, stageDef, pipelineConf);
          } else {
            return injectSimpleConfiguration(var, conf, confDef, stageDef, pipelineConf);
          }
        } catch (ClassNotFoundException | NoSuchFieldException e) {
          throw new RuntimeException(Utils.format(ContainerError.CONTAINER_0152.getMessage(),
            stageDef.getClassName(), stageConf.getInstanceName(), confDef.getFieldName(),
            conf.getValue(), e.getMessage(), e));
        } catch (ELEvalException e) {
          issues.add(issueCreator.createConfigIssue(stageConf.getInstanceName(), confDef.getGroup(), confDef.getName(),
            ValidationError.VALIDATION_0033, e.getMessage(), e));
          return null;
        }
    }
  }

  private ConfigConfiguration injectComplexConfiguration(Field var, ConfigConfiguration conf,
                                                         ConfigDefinition confDef,
                                                        StageDefinition stageDef, PipelineConfiguration pipelineConf)
    throws NoSuchFieldException, ELEvalException {
    Type genericType = var.getGenericType();
    Class klass;
    if(genericType instanceof ParameterizedType) {
      Type[] typeArguments = ((ParameterizedType) genericType).getActualTypeArguments();
      klass = (Class) typeArguments[0];
    } else {
      klass = (Class) genericType;
    }

    //value is a list of map where each map has keys which represent the field names in custom config object and the
    // value is the value of the field
    if(conf.getValue() instanceof List) {
      List<Map<String, Object>> evaluatedValue = new ArrayList<>();
      for(Object object : (List) conf.getValue()) {
        if(object instanceof Map) {
          Map<String, Object> map = (Map) object;
          Map<String, Object> evaluatedMap = new HashMap<>();
          for (ConfigDefinition configDefinition : confDef.getModel().getConfigDefinitions()) {
            Field f = klass.getField(configDefinition.getFieldName());
            evaluatedMap.put(configDefinition.getFieldName(),
              getValueToInject(f, map.get(configDefinition.getFieldName()), configDefinition, stageDef, pipelineConf));
          }
          evaluatedValue.add(evaluatedMap);
        }
      }
      conf = new ConfigConfiguration(conf.getName(), evaluatedValue);
    }
    return conf;
  }

  private ConfigConfiguration injectSimpleConfiguration(Field var, ConfigConfiguration conf,
                                                        ConfigDefinition confDef,
                                                        StageDefinition stageDef, PipelineConfiguration pipelineConf) throws ELEvalException {
    Object value = getValueToInject(var, conf.getValue(), confDef, stageDef, pipelineConf);
    if(value != conf.getValue()) {
      conf = new ConfigConfiguration(conf.getName(), value);
    }
    return conf;
  }

  @VisibleForTesting
  boolean validatePipelineConfiguration(StageIssueCreator issueCreator) {
    boolean preview = true;
    Set<String> stageNames = new HashSet<>();
    boolean shouldBeSource = true;
    for (StageConfiguration stageConf : pipelineConfiguration.getStages()) {
      if (stageNames.contains(stageConf.getInstanceName())) {
        // duplicate stage instance name in the pipeline
        issues.add(issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0005));
        preview = false;
      }
      preview &= validateStageConfiguration(shouldBeSource, stageConf, false, StageIssueCreator.getStageCreator());
      stageNames.add(stageConf.getInstanceName());
      shouldBeSource = false;
    }
    return preview;
  }

  private boolean validateModel(StageConfiguration stageConf, StageDefinition stageDef, ConfigDefinition confDef, ConfigConfiguration conf,
      StageIssueCreator issueCreator) {
    String instanceName = stageConf.getInstanceName();
    boolean preview = true;
    switch (confDef.getModel().getModelType()) {
      case VALUE_CHOOSER:
        if(!(conf.getValue() instanceof String || conf.getValue().getClass().isEnum()) ) {
          // stage configuration must be a model
          issues.add(issueCreator.createConfigIssue(instanceName, confDef.getGroup(), confDef.getName(),
                                                  ValidationError.VALIDATION_0009, "String"));
          preview = false;
        }
        break;
      case FIELD_SELECTOR_MULTI_VALUED:
        if(!(conf.getValue() instanceof List)) {
          // stage configuration must be a model
          issues.add(issueCreator.createConfigIssue(instanceName, confDef.getGroup(), confDef.getName(),
                                                  ValidationError.VALIDATION_0009, "List"));
          preview = false;
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
          issues.add(issueCreator.createConfigIssue(instanceName, confDef.getGroup(), confDef.getName(),
                                                  ValidationError.VALIDATION_0009, "Map"));
          preview = false;
        }
        break;
      case LANE_PREDICATE_MAPPING:
        if(!(conf.getValue() instanceof List)) {
          // stage configuration must be a model
          issues.add(issueCreator.createConfigIssue(instanceName, confDef.getGroup(), confDef.getName(),
                                                    ValidationError.VALIDATION_0009, "List<Map>"));
          preview = false;
        } else {
          int count = 1;
          for (Object element : (List) conf.getValue()) {
            if (element instanceof Map) {
              Map map = (Map)element;
              if (!map.containsKey("outputLane")) {
                issues.add(issueCreator.createConfigIssue(instanceName, confDef.getGroup(), confDef.getName(),
                                                        ValidationError.VALIDATION_0020, count, "outputLane"));
                preview = false;
              } else {
                if (map.get("outputLane") == null) {
                  issues.add(issueCreator.createConfigIssue(instanceName, confDef.getGroup(), confDef.getName(),
                                                          ValidationError.VALIDATION_0021, count, "outputLane"));
                  preview = false;
                } else {
                  if (!(map.get("outputLane") instanceof String)) {
                    issues.add(issueCreator.createConfigIssue(instanceName, confDef.getGroup(), confDef.getName(),
                                                            ValidationError.VALIDATION_0022, count, "outputLane"));
                    preview = false;
                  } else if (((String)map.get("outputLane")).isEmpty()) {
                    issues.add(issueCreator.createConfigIssue(instanceName, confDef.getGroup(), confDef.getName(),
                                                            ValidationError.VALIDATION_0023, count, "outputLane"));
                    preview = false;
                  }
                }
              }
              if (!map.containsKey("predicate")) {
                issues.add(issueCreator.createConfigIssue(instanceName, confDef.getGroup(), confDef.getName(),
                                                        ValidationError.VALIDATION_0020, count, "condition"));
                preview = false;
              } else {
                if (map.get("predicate") == null) {
                  issues.add(issueCreator.createConfigIssue(instanceName, confDef.getGroup(), confDef.getName(),
                                                          ValidationError.VALIDATION_0021, count, "condition"));
                  preview = false;
                } else {
                  if (!(map.get("predicate") instanceof String)) {
                    issues.add(issueCreator.createConfigIssue(instanceName, confDef.getGroup(), confDef.getName(),
                                                            ValidationError.VALIDATION_0022, count, "condition"));
                    preview = false;
                  } else if (((String)map.get("predicate")).isEmpty()) {
                    issues.add(issueCreator.createConfigIssue(instanceName, confDef.getGroup(), confDef.getName(),
                                                            ValidationError.VALIDATION_0023, count, "condition"));
                    preview = false;
                  }
                }
              }
            } else {
              issues.add(issueCreator.createConfigIssue(instanceName, confDef.getGroup(), confDef.getName(),
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
                                        StageDefinition stageDef, StageIssueCreator issueCreator) {
    boolean preview = true;
    for(Map.Entry<String, Object> entry : confvalue.entrySet()) {
      String configName = entry.getKey();
      Object value = entry.getValue();
      ConfigDefinition configDefinition = configDefinitionsMap.get(configName);
      ConfigConfiguration configConfiguration = new ConfigConfiguration(configName, value);
      preview &= validateConfigDefinition(configDefinition, configConfiguration, stageConf, stageDef, confvalue,
        issueCreator, false /*do not inject*/);
    }
    return preview;
  }


  @VisibleForTesting
  boolean validatePipelineLanes(StageIssueCreator issueCreator) {
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
          issues.add(issueCreator.createStageIssue(downStreamStageConf.getInstanceName(),
                                                 ValidationError.VALIDATION_0010,
                                                 duplicateOutputs, stageConf.getInstanceName()));
          preview = false;
        }

        openOutputs.removeAll(downStreamStageConf.getInputLanes());
      }
      if (!openOutputs.isEmpty()) {
        openLanes.addAll(openOutputs);
        // the stage has open output lanes
        StageIssue issue = issueCreator.createStageIssue(stageConf.getInstanceName(), ValidationError.VALIDATION_0011);
        issue.setAdditionalInfo("openStreams", openOutputs);
        issues.add(issue);
      }
    }
    return preview;
  }

  @VisibleForTesting
  boolean validateErrorStage() {
    boolean preview = true;
    if (pipelineConfiguration.getErrorStage() == null) {
      issues.addP(new Issue(PipelineDefConfigs.ERROR_RECORDS_CONFIG, PipelineDefConfigs.Groups.BAD_RECORDS.name(),
                            ValidationError.VALIDATION_0060));
      preview = false;
    } else {
      StageConfiguration errorStage = pipelineConfiguration.getErrorStage();
      StageIssueCreator errorStageCreator = StageIssueCreator.getErrorStageCreator();
      preview &= validateStageConfiguration(false, errorStage, true, errorStageCreator);
      preview &= validateStagesExecutionMode(errorStageCreator, Arrays.asList(errorStage));
    }
    return preview;
  }

  private Object getValueToInject(Field var, Object configValue, ConfigDefinition confDef,
                                  StageDefinition stageDef, PipelineConfiguration pipelineConf) throws ELEvalException {
    Object value = configValue;
    if (configValue instanceof Map) {
      Map<Object, Object> evaluatedMap = new HashMap<>();
      for(Map.Entry<Object, Object> e : ((Map<Object, Object>)configValue).entrySet()) {
        evaluatedMap.put(e.getKey(), ElUtil.evaluate(e.getValue(), var, stageDef, confDef, constants));
      }
      value = evaluatedMap;
    } else if (configValue instanceof List) {
      if (confDef.getType() == ConfigDef.Type.LIST) {
        List<Object> evaluatedValue = new ArrayList<>();
        for (Object e : (List) configValue) {
          evaluatedValue.add(ElUtil.evaluate(e, var, stageDef, confDef, constants));
        }
        value = evaluatedValue;
      } else if (confDef.getType() == ConfigDef.Type.MAP) {
        //List of Map
        List<Map> evaluatedList = new ArrayList<>();
        List<Map> list = (List<Map>) configValue;
        for(Map map : list) {
          //elements of map are always Strings
          Map<Object, Object> evaluatedMap = new HashMap<>();
          Object key = map.get("key");
          Object val = map.get("value");
          evaluatedMap.put("key", key);
          evaluatedMap.put("value", ElUtil.evaluate(val, var, stageDef, confDef, constants));
          evaluatedList.add(evaluatedMap);
        }
        value = evaluatedList;
      }
    } else {
      value = ElUtil.evaluate(configValue, var, stageDef, confDef, constants);
    }
    return value;
  }
}
