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
import com.streamsets.datacollector.config.PipelineConfiguration;
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
            .filter(s -> s.getProvides().equals(serviceDependencyDefinition.getService()))
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

}
