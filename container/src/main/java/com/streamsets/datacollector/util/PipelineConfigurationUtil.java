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
package com.streamsets.datacollector.util;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ModelDefinition;
import com.streamsets.datacollector.config.ModelType;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineDefinition;
import com.streamsets.datacollector.config.ServiceConfiguration;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.ServiceDependencyDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PipelineConfigurationUtil {

  private static final String KEY = "key";
  private static final String VALUE = "value";

  private static final String SPARK_PROCESSOR_STAGE = "com_streamsets_pipeline_stage_processor_spark_SparkDProcessor";

  private PipelineConfigurationUtil() {}

  public static Map<String, String> getFlattenedStringMap(String mapName, PipelineConfiguration pipelineConf) {
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, Object> entry : getFlattenedMap(mapName, pipelineConf.getConfiguration()).entrySet()) {
      result.put(entry.getKey(), String.valueOf(entry.getValue()));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> getFlattenedMap(String mapName, List<Config> pipelineConf) {
    Map<String, Object> constants = new HashMap<>();
    if(pipelineConf != null) {
      for (Config config : pipelineConf) {
        if (mapName.equals(config.getName()) && config.getValue() != null) {
          for (Map<String, String> map : (List<Map<String, String>>) config.getValue()) {
            constants.put(map.get(KEY), map.get(VALUE));
          }
          return constants;
        }
      }
    }
    return constants;
  }

  private static PipelineConfiguration getPipelineConfiguration(String pipelineJson) throws IOException {
    ObjectMapper json = ObjectMapperFactory.getOneLine();
    PipelineConfigurationJson pipelineConfigBean = json.readValue(pipelineJson, PipelineConfigurationJson.class);
    return BeanHelper.unwrapPipelineConfiguration(pipelineConfigBean);
  }

  public static String getSourceLibName(String pipelineJson) throws JsonParseException, JsonMappingException,
      IOException {
    PipelineConfiguration pipelineConf = getPipelineConfiguration(pipelineJson);
    StageConfiguration stageConfiguration = Utils.checkNotNull(getSourceStageConf(pipelineConf), "StageConfiguration" +
        "for origin");
    return stageConfiguration.getLibrary();
  }

  @SuppressWarnings("unchecked")
  public static List<SparkTransformerConfig> getSparkTransformers(String pipelineJson) throws Exception {
    PipelineConfiguration pipelineConf = getPipelineConfiguration(pipelineJson);
    return
        pipelineConf.getStages().stream()
            .filter(stageConfiguration -> stageConfiguration.getStageName().equals(SPARK_PROCESSOR_STAGE))
            .map(stageConfiguration -> {
              String transformerClass =
                  stageConfiguration.getConfig("sparkProcessorConfigBean.transformerClass").getValue().toString();
              List<String> parameters =
                  (List<String>) stageConfiguration.getConfig("sparkProcessorConfigBean.preprocessMethodArgs").getValue();
              return new SparkTransformerConfig(transformerClass, parameters);
            }).collect(Collectors.toList());
  }

  public static List<String> getSparkProcessorConf(String pipelineJson) throws Exception {
    PipelineConfiguration pipelineConf = getPipelineConfiguration(pipelineJson);
    return pipelineConf.getStages().stream()
        .filter(stageConfiguration -> stageConfiguration.getStageName().equals(SPARK_PROCESSOR_STAGE))
        .map(StageConfiguration::getLibrary)
        .collect(Collectors.toList());
  }

  public static StageConfiguration getSourceStageConf(PipelineConfiguration pipelineConf) {
    for (int i = 0; i < pipelineConf.getStages().size(); i++) {
      StageConfiguration stageConf = pipelineConf.getStages().get(i);
      if (stageConf.getInputLanes().isEmpty()) {
        return stageConf;
      }
    }
    return null;
  }

  public static StageConfiguration getStageConfigurationWithDefaultValues(
      StageLibraryTask stageLibraryTask,
      String library,
      String stageName,
      String stageInstanceName,
      String labelPrefix
  ) {
    StageDefinition stageDefinition = stageLibraryTask.getStage(library, stageName, false);

    if (stageDefinition == null) {
      return null;
    }

    List<Config> configurationList = new ArrayList<>();
    for (ConfigDefinition configDefinition : stageDefinition.getConfigDefinitions()) {
      configurationList.add(getConfigWithDefaultValue(configDefinition));
    }

    List<ServiceConfiguration> serviceConfigurationList = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(stageDefinition.getServices())) {
      List<ServiceDefinition> serviceDefinitions = stageLibraryTask.getServiceDefinitions();
      for(ServiceDependencyDefinition serviceDependencyDefinition: stageDefinition.getServices()) {
        ServiceDefinition serviceDefinition = serviceDefinitions.stream()
            .filter(s -> s.getProvides().equals(serviceDependencyDefinition.getServiceClass()))
            .findAny()
            .orElse(null);

        if (serviceDefinition != null) {
          List<Config> serviceConfigList = new ArrayList<>();
          for (ConfigDefinition configDefinition : serviceDefinition.getConfigDefinitions()) {
            if (serviceDependencyDefinition.getConfiguration() != null &&
                serviceDependencyDefinition.getConfiguration().containsKey(configDefinition.getName())) {
              serviceConfigList.add(new Config(
                  configDefinition.getName(),
                  serviceDependencyDefinition.getConfiguration().get(configDefinition.getName())
              ));
            } else {
              serviceConfigList.add(getConfigWithDefaultValue(configDefinition));
            }
          }
          serviceConfigurationList.add(new ServiceConfiguration(
              serviceDefinition.getProvides(),
              serviceDefinition.getVersion(),
              serviceConfigList
          ));
        }
      }
    }

    return new StageConfiguration(
        stageInstanceName,
        library,
        stageName,
        stageDefinition.getVersion(),
        configurationList,
        ImmutableMap.of(
            "label", labelPrefix + stageDefinition.getLabel(),
            "stageType", stageDefinition.getType().toString()
        ),
        serviceConfigurationList,
        new ArrayList<>(),
        new ArrayList<>(),
        new ArrayList<>()
    );
  }

  private static Config getConfigWithDefaultValue(ConfigDefinition configDefinition) {
    switch (configDefinition.getType()) {
      case MODEL:
        ModelDefinition modelDefinition = configDefinition.getModel();
        switch (modelDefinition.getModelType()) {
          case FIELD_SELECTOR_MULTI_VALUE:
            return new Config(configDefinition.getName(), Collections.emptyList());
          case LIST_BEAN:
            Map<String, Object> listBeanDefaultValue = new HashMap<>();
            for (ConfigDefinition modelConfigDefinition : modelDefinition.getConfigDefinitions()) {
              Config listBeanConfig = getConfigWithDefaultValue(modelConfigDefinition);
              listBeanDefaultValue.put(modelConfigDefinition.getName(), listBeanConfig.getValue());
            }
            return new Config(configDefinition.getName(), ImmutableList.of(listBeanDefaultValue));
          default:
            break;
        }
        break;
      case MAP:
      case LIST:
        return new Config(configDefinition.getName(), Collections.emptyList());
      case BOOLEAN:
        return new Config(configDefinition.getName(), false);
      default:
        return new Config(configDefinition.getName(), configDefinition.getDefaultValue());
    }
    return new Config(configDefinition.getName(), configDefinition.getDefaultValue());
  }

  public static void stripPipelineConfigPlainCredentials(
      PipelineConfiguration pipelineConfiguration,
      StageLibraryTask stageLibrary
  ) {
    // pipeline configs
    PipelineDefinition pipelineDefinition = stageLibrary.getPipeline();
    List<Config> newConfigs = stripConfigPlainCredentials(
        pipelineConfiguration.getConfiguration(),
        getMapOfConfigDefinitions(pipelineDefinition.getConfigDefinitions())
    );
    pipelineConfiguration.getConfiguration().clear();
    pipelineConfiguration.getConfiguration().addAll(newConfigs);

    // stages configs
    pipelineConfiguration.getStages().forEach(stageInstances ->
        stripStageConfigPlainCredentials(stageInstances, stageLibrary));

    // special stage - error stage
    if (pipelineConfiguration.getErrorStage() != null) {
      stripStageConfigPlainCredentials(pipelineConfiguration.getErrorStage(), stageLibrary);
    }

    // special stage - stats aggr stage
    if (pipelineConfiguration.getStatsAggregatorStage() != null) {
      stripStageConfigPlainCredentials(pipelineConfiguration.getStatsAggregatorStage(), stageLibrary);
    }

    // special stage - test origin
    if (pipelineConfiguration.getTestOriginStage() != null) {
      stripStageConfigPlainCredentials(pipelineConfiguration.getTestOriginStage(), stageLibrary);
    }

    // special stage - start event
    if (!pipelineConfiguration.getStartEventStages().isEmpty()) {
      stripStageConfigPlainCredentials(pipelineConfiguration.getStartEventStages().get(0), stageLibrary);
    }

    // special stage - stop event
    if (!pipelineConfiguration.getStopEventStages().isEmpty()) {
      stripStageConfigPlainCredentials(pipelineConfiguration.getStopEventStages().get(0), stageLibrary);
    }
  }

  private static void stripStageConfigPlainCredentials(
      StageConfiguration stageInstance,
      StageLibraryTask stageLibrary
  ) {
    StageDefinition stageDefinition = stageLibrary.getStage(
        stageInstance.getLibrary(),
        stageInstance.getStageName(),
        false
    );
    if (stageDefinition != null) {
      List<Config> newStageConfigs = stripConfigPlainCredentials(
          stageInstance.getConfiguration(),
          getMapOfConfigDefinitions(stageDefinition.getConfigDefinitions())
      );
      stageInstance.setConfig(newStageConfigs);

      // Handle Services
      if (CollectionUtils.isNotEmpty(stageDefinition.getServices())) {
        List<ServiceDefinition> serviceDefinitions = stageLibrary.getServiceDefinitions();
        for(ServiceDependencyDefinition serviceDependencyDefinition: stageDefinition.getServices()) {
          ServiceDefinition serviceDefinition = serviceDefinitions.stream()
              .filter(s -> s.getProvides().equals(serviceDependencyDefinition.getServiceClass()))
              .findAny()
              .orElse(null);
          ServiceConfiguration serviceConfiguration = stageInstance.getServices().stream()
              .filter(s -> s.getService().equals(serviceDependencyDefinition.getServiceClass()))
              .findAny()
              .orElse(null);

          if (serviceDefinition != null && serviceConfiguration != null) {
            List<Config> newServiceConfigs = stripConfigPlainCredentials(
                serviceConfiguration.getConfiguration(),
                getMapOfConfigDefinitions(serviceDefinition.getConfigDefinitions())
            );
            serviceConfiguration.setConfig(newServiceConfigs);
          }
        }
      }
    }
  }

  private static List<Config> stripConfigPlainCredentials(
      List<Config> configList,
      Map<String, ConfigDefinition> configDefinitionMap
  ) {
    Map<String, Config> replacementConfigMap = new HashMap<>();
    configList.forEach(config -> {
      if (configDefinitionMap.containsKey(config.getName())) {
        ConfigDefinition configDefinition = configDefinitionMap.get(config.getName());
        if (configDefinition.getType().equals(ConfigDef.Type.CREDENTIAL) && !ElUtil.isElString(config.getValue())) {
          replacementConfigMap.put(config.getName(), new Config(config.getName(), ""));
        } else if (configDefinition.getModel() != null &&
            configDefinition.getModel().getModelType().equals(ModelType.LIST_BEAN)) {

          Map<String, ConfigDefinition> listBeanConfigDefinitionMap =
              getMapOfConfigDefinitions(configDefinition.getModel().getConfigDefinitions());
          if (config.getValue() != null) {
            if (config.getValue() instanceof List) {
              // list of hash maps
              List<Map<String, Object>> maps = (List<Map<String, Object>>) config.getValue();
              for (Map<String, Object> map : maps) {
                stripListBeanConfigPlainCredentials(listBeanConfigDefinitionMap, map);
              }
            } else if (config.getValue() instanceof Map) {
              Map<String, Object> listBeanConfigValue = (Map<String, Object>) config.getValue();
              stripListBeanConfigPlainCredentials(listBeanConfigDefinitionMap, listBeanConfigValue);
            }
          }
        }
      }
    });
    return createWithNewConfigs(configList, replacementConfigMap);
  }

  private static void stripListBeanConfigPlainCredentials(
      Map<String, ConfigDefinition> listBeanConfigDefinitionMap,
      Map<String, Object> listBeanConfigValue
  ) {
    for (Map.Entry<String, Object> entry : listBeanConfigValue.entrySet()) {
      String configName = entry.getKey();
      Object value = entry.getValue();
      ConfigDefinition listBeanConfigDefinition = listBeanConfigDefinitionMap.get(configName);
      if (listBeanConfigDefinition.getType().equals(ConfigDef.Type.CREDENTIAL) &&
          !ElUtil.isElString(value)) {
        entry.setValue("");
      }
    }
  }

  private static Map<String, ConfigDefinition> getMapOfConfigDefinitions(List<ConfigDefinition> configDefinitions) {
    Map<String, ConfigDefinition> configDefinitionMap = new HashMap<>();
    configDefinitions.forEach(c -> configDefinitionMap.put(c.getName(), c));
    return configDefinitionMap;
  }

  private static List<Config> createWithNewConfigs(List<Config> configs, Map<String, Config> replacementConfigMap) {
    List<Config> newConfigurations = new ArrayList<>();
    for (Config candidate : configs) {
      newConfigurations.add(replacementConfigMap.getOrDefault(candidate.getName(), candidate));
    }
    return newConfigurations;
  }

}
