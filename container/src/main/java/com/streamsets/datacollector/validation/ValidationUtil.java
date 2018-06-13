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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ServiceConfiguration;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.UserConfigurable;
import com.streamsets.datacollector.creation.StageConfigBean;
import com.streamsets.datacollector.definition.ConcreteELDefinitionExtractor;
import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.record.PathElement;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageType;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.impl.TextUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reusable methods for various pipeline and stage validation.
 */
public class ValidationUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ValidationUtil.class);

  /**
   * Resolve stage aliases (e.g. when a stage is renamed).
   */
  public static void resolveStageAlias(
    StageLibraryTask stageLibrary,
    StageConfiguration stageConf
  ) {
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

  /**
   * Resolve all stage library relevant aliases - this includes:
   *
   * * Change in stage library name - e.g. when a stage moves from let say cdh5_cluster_2 to cdh_5_spark2)
   *
   * * Change in stage name - e.g. when a stage class is renamed
   */
  public static boolean resolveLibraryAliases(
    StageLibraryTask stageLibrary,
    List<StageConfiguration> stageConfigurations
  ) {
    for (StageConfiguration stageConf : stageConfigurations) {
      String name = stageConf.getLibrary();
      if (stageLibrary.getLibraryNameAliases().containsKey(name)) {
        stageConf.setLibrary(stageLibrary.getLibraryNameAliases().get(name));
      }
      resolveStageAlias(stageLibrary, stageConf);
    }
    return true;
  }

  /**
   * Add any missing configs to the stage configuration.
   */
  public static void addMissingConfigsToStage(
    StageLibraryTask stageLibrary,
    StageConfiguration stageConf
  ) {
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

  /**
   * Validate given stage configuration.
   */
  public static boolean validateStageConfiguration(
    StageLibraryTask stageLibrary,
    boolean shouldBeSource,
    StageConfiguration stageConf,
    boolean notOnMainCanvas,
    IssueCreator issueCreator,
    boolean isPipelineFragment,
    Map<String, Object> constants,
    List<Issue> issues
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

      // Hidden stages can't appear on the main canvas
      if(!notOnMainCanvas && !stageDef.getHideStage().isEmpty()) {
        issues.add(issueCreator.create(stageConf.getInstanceName(), ValidationError.VALIDATION_0037));
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
          if (!notOnMainCanvas && stageConf.getInputLanes().isEmpty() && !isPipelineFragment) {
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
          if(!notOnMainCanvas) {
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
          }
          break;
        case EXECUTOR:
        case TARGET:
          // Normal target stage must have at least one input lane
          if (!notOnMainCanvas && stageConf.getInputLanes().isEmpty() && !isPipelineFragment) {
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
          if (notOnMainCanvas && !stageConf.getInputLanes().isEmpty()) {
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
          if (notOnMainCanvas && !stageConf.getEventLanes().isEmpty()) {
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
        constants,
        issueCreator,
        issues
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
            constants,
            issueCreator.forService(serviceConf.getService().getName()),
            issues
          );
        }
      }
    }
    return preview;
  }


  private static boolean validateComponentConfigs(
    UserConfigurable component,
    List<ConfigDefinition> configs,
    Map<String, ConfigDefinition> definitionMap,
    Set<String> hideConfigs,
    boolean validatePrecondition,
    Map<String, Object> constants,
    IssueCreator issueCreator,
    List<Issue> issues
  ) {
    boolean preview = true;

    // Validate user exposed configuration
    for (ConfigDefinition confDef : configs) {
      Config config = component.getConfig(confDef.getName());
      if (confDef.isRequired() && (config == null || isNullOrEmpty(confDef, config))) {
        preview &= validateRequiredField(confDef, component, issueCreator, issues);
      }

      if(confDef.getType() == ConfigDef.Type.NUMBER && !isNullOrEmpty(confDef, config)) {
        preview &= validatedNumberConfig(config, confDef, component, issueCreator, issues);
      }
    }

    for (Config conf : component.getConfiguration()) {
      ConfigDefinition confDef = definitionMap.get(conf.getName());
      preview &= validateConfigDefinition(confDef, hideConfigs, conf, component, definitionMap, null, issueCreator, issues);
      if (confDef != null && validatePrecondition && confDef.getName().equals(StageConfigBean.STAGE_PRECONDITIONS_CONFIG)) {
        preview &= validatePreconditions(confDef, conf, constants, issueCreator, issues);
      }
    }

    return preview;
  }

  public static boolean isNullOrEmpty(ConfigDefinition confDef, Config config) {
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

  public static boolean validateRequiredField(
      ConfigDefinition confDef,
      UserConfigurable component,
      IssueCreator issueCreator,
      List<Issue> issues
  ) {
    boolean preview = true;

    // If the config doesn't depend on anything or the config should be triggered, config is invalid
    if (confDef.getDependsOnMap().isEmpty() || configTriggered(component, confDef)) {
      issues.add(issueCreator.create(confDef.getGroup(), confDef.getName(), ValidationError.VALIDATION_0007));
      preview = false;
    }
    return preview;
  }

  private static final Record PRECONDITION_RECORD = new RecordImpl("dummy", "dummy", null, null);

  @SuppressWarnings("unchecked")
  private static boolean validatePreconditions(
      ConfigDefinition confDef,
      Config conf,
      Map<String, Object> constants,
      IssueCreator issueCreator,
      List<Issue> issues
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

  private static boolean validateConfigDefinition(
      ConfigDefinition confDef,
      Set<String> hideConfigs,
      Config conf,
      UserConfigurable stageConf,
      Map<String, ConfigDefinition> definitionMap,
      Map<String, Object> parentConf,
      IssueCreator issueCreator,
      List<Issue> issues
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
        preview = validateModel(stageConf, definitionMap, hideConfigs, confDef, conf, issueCreator, issues);
      }
    }
    return preview;
  }

  private static Config getConfig(List<Config> configs, String name) {
    for (Config config : configs) {
      if (config.getName().equals(name)) {
        return config;
      }
    }
    return null;
  }



  private static boolean configTriggered(UserConfigurable component, ConfigDefinition confDef) {
    boolean triggered = true;

    for (Map.Entry<String, List<Object>> dependency : confDef.getDependsOnMap().entrySet()) {
      //At times the dependsOn config may be hidden [for ex. ToErrorKafkaTarget hides the dataFormat property].
      // In such a scenario component.getConfig(dependsOn) can be null. We need to guard against this.
      triggered &= (component.getConfig(dependency.getKey()) != null &&
          triggeredByContains(dependency.getValue(), component.getConfig(dependency.getKey()).getValue()));
    }
    return triggered;
  }

  public static boolean validatedNumberConfig(
      Config conf,
      ConfigDefinition confDef,
      UserConfigurable component,
      IssueCreator issueCreator,
      List<Issue> issues
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

  private static boolean triggeredByContains(List<Object> triggeredBy, Object value) {
    boolean contains = false;
    for (Object object : triggeredBy) {
      if (String.valueOf(object).equals(String.valueOf(value))) {
        contains = true;
        break;
      }
    }
    return contains;
  }

  @SuppressWarnings("unchecked")
  private static boolean validateModel(
      UserConfigurable stageConf,
      Map<String, ConfigDefinition> definitionMap,
      Set<String> hideConfigs,
      ConfigDefinition confDef,
      Config conf,
      IssueCreator issueCreator,
      List<Issue> issues
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
              preview &= validateComplexConfig(configDefinitionsMap, map, stageConf, definitionMap, hideConfigs, issueCreator, issues);
            }
          } else if (conf.getValue() instanceof Map) {
            preview &= validateComplexConfig(
                configDefinitionsMap,
                (Map<String, Object>) conf.getValue(),
                stageConf,
                definitionMap,
                hideConfigs,
                issueCreator,
                issues
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

  private static boolean validateComplexConfig(
      Map<String, ConfigDefinition> configDefinitionsMap,
      Map<String, Object> confvalue,
      UserConfigurable stageConf,
      Map<String, ConfigDefinition> definitionMap,
      Set<String> hideConfigs,
      IssueCreator issueCreator,
      List<Issue> issues
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
          issueCreator,
          issues
      );
    }
    return preview;
  }

  private ValidationUtil() {
  }
}
