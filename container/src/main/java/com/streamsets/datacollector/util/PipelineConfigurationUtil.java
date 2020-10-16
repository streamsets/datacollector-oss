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
import com.google.common.io.BaseEncoding;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ModelDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineFragmentConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.ServiceConfiguration;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.ServiceDependencyDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.DefinitionsJson;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.PipelineDefinitionJson;
import com.streamsets.datacollector.restapi.bean.PipelineEnvelopeJson;
import com.streamsets.datacollector.restapi.bean.PipelineFragmentDefinitionJson;
import com.streamsets.datacollector.restapi.bean.PipelineFragmentEnvelopeJson;
import com.streamsets.datacollector.restapi.bean.PipelineRulesDefinitionJson;
import com.streamsets.datacollector.restapi.bean.StageDefinitionJson;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.credential.PipelineCredentialHandler;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PipelineConfigurationUtil {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigurationUtil.class);
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String PIPELINE_ID_REGEX = "[\\W]|_";

  private static final String SPARK_PROCESSOR_STAGE = "com_streamsets_pipeline_stage_processor_spark_SparkDProcessor";

  private PipelineConfigurationUtil() {}

  public static String generatePipelineId(String pipelineTitle) {
    return pipelineTitle
        .substring(0, Math.min(pipelineTitle.length(), 10))
        .replaceAll(PIPELINE_ID_REGEX, "") + UUID.randomUUID().toString();
  }

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

  public static PipelineEnvelopeJson getPipelineEnvelope(
      StageLibraryTask stageLibrary,
      CredentialStoresTask credentialStoresTask,
      Configuration configuration,
      PipelineConfiguration pipelineConfig,
      RuleDefinitions ruleDefinitions,
      boolean includeLibraryDefinitions,
      boolean includePlainTextCredentials
  ) {
    //Always auto decrypt credentials stored by managed store
    PipelineCredentialHandler.getDecrypter(stageLibrary, credentialStoresTask, configuration)
        .handlePipelineConfigCredentials(pipelineConfig);

    if (!includePlainTextCredentials) {
      PipelineCredentialHandler.getPlainTextScrubber(stageLibrary)
          .handlePipelineConfigCredentials(pipelineConfig);
    }

    PipelineEnvelopeJson pipelineEnvelope = new PipelineEnvelopeJson();
    pipelineEnvelope.setPipelineConfig(BeanHelper.wrapPipelineConfiguration(pipelineConfig));
    pipelineEnvelope.setPipelineRules(BeanHelper.wrapRuleDefinitions(ruleDefinitions));
    if (includeLibraryDefinitions) {
      DefinitionsJson definitions = new DefinitionsJson();

      // Add only stage definitions for stages present in pipeline config
      List<StageDefinition> stageDefinitions = new ArrayList<>();
      Map<String, String> stageIcons = new HashMap<>();

      for (StageConfiguration conf : pipelineConfig.getOriginalStages()) {
        fetchStageDefinition(stageLibrary, conf, stageDefinitions, stageIcons);
      }

      // add from fragments
      if (CollectionUtils.isNotEmpty(pipelineConfig.getFragments())) {
        pipelineConfig.getFragments().forEach(pipelineFragmentConfiguration -> {
          for (StageConfiguration conf : pipelineFragmentConfiguration.getOriginalStages()) {
            fetchStageDefinition(stageLibrary, conf, stageDefinitions, stageIcons);
          }
        });
      }

      StageConfiguration errorStageConfig = pipelineConfig.getErrorStage();
      if (errorStageConfig != null) {
        fetchStageDefinition(stageLibrary, errorStageConfig, stageDefinitions, stageIcons);
      }

      StageConfiguration originStageConfig = pipelineConfig.getTestOriginStage();
      if (originStageConfig != null) {
        fetchStageDefinition(stageLibrary, originStageConfig, stageDefinitions, stageIcons);
      }

      StageConfiguration statsAggregatorStageConfig = pipelineConfig.getStatsAggregatorStage();
      if (statsAggregatorStageConfig != null) {
        fetchStageDefinition(stageLibrary, statsAggregatorStageConfig, stageDefinitions, stageIcons);
      }

      for (StageConfiguration startEventStage: pipelineConfig.getStartEventStages()) {
        fetchStageDefinition(stageLibrary, startEventStage, stageDefinitions, stageIcons);
      }

      for (StageConfiguration stopEventStage: pipelineConfig.getStopEventStages()) {
        fetchStageDefinition(stageLibrary, stopEventStage, stageDefinitions, stageIcons);
      }

      List<StageDefinitionJson> stages = new ArrayList<>(BeanHelper.wrapStageDefinitions(stageDefinitions));
      definitions.setStages(stages);

      definitions.setStageIcons(stageIcons);

      List<PipelineDefinitionJson> pipeline = new ArrayList<>(1);
      pipeline.add(BeanHelper.wrapPipelineDefinition(stageLibrary.getPipeline()));
      definitions.setPipeline(pipeline);

      List<PipelineRulesDefinitionJson> pipelineRules = new ArrayList<>(1);
      pipelineRules.add(BeanHelper.wrapPipelineRulesDefinition(stageLibrary.getPipelineRules()));
      definitions.setPipelineRules(pipelineRules);

      Map<Class, ServiceDefinition> serviceByClass = stageLibrary.getServiceDefinitions().stream()
          .collect(Collectors.toMap(ServiceDefinition::getProvides, Function.identity()));

      List<ServiceDefinition> pipelineServices = stageDefinitions.stream()
          .flatMap(stageDefinition -> stageDefinition.getServices().stream())
          .map(ServiceDependencyDefinition::getServiceClass)
          .distinct()
          .map(serviceClass -> serviceByClass.get(serviceClass))
          .filter(Objects::nonNull)
          .collect(Collectors.toList());

      definitions.setServices(BeanHelper.wrapServiceDefinitions(pipelineServices));

      pipelineEnvelope.setLibraryDefinitions(definitions);
    }

    return pipelineEnvelope;
  }

  public static PipelineFragmentEnvelopeJson getPipelineFragmentEnvelope(
      StageLibraryTask stageLibrary,
      PipelineFragmentConfiguration pipelineFragmentConfig,
      RuleDefinitions ruleDefinitions,
      boolean includeLibraryDefinitions
  ) {
    PipelineFragmentEnvelopeJson pipelineFragmentEnvelope = new PipelineFragmentEnvelopeJson();
    pipelineFragmentEnvelope.setPipelineFragmentConfig(
        BeanHelper.wrapPipelineFragmentConfiguration(pipelineFragmentConfig)
    );
    pipelineFragmentEnvelope.setPipelineRules(BeanHelper.wrapRuleDefinitions(ruleDefinitions));
    if (includeLibraryDefinitions) {
      DefinitionsJson definitions = new DefinitionsJson();

      // Add only stage definitions for stages present in pipeline config
      List<StageDefinition> stageDefinitions = new ArrayList<>();
      Map<String, String> stageIcons = new HashMap<>();

      for (StageConfiguration conf : pipelineFragmentConfig.getStages()) {
        fetchStageDefinition(stageLibrary, conf, stageDefinitions, stageIcons);
      }

      StageConfiguration originStageConfig = pipelineFragmentConfig.getTestOriginStage();
      if (originStageConfig != null) {
        fetchStageDefinition(stageLibrary, originStageConfig, stageDefinitions, stageIcons);
      }

      // add from fragments
      if (CollectionUtils.isNotEmpty(pipelineFragmentConfig.getFragments())) {
        pipelineFragmentConfig.getFragments().forEach(pipelineFragmentConfiguration -> {
          for (StageConfiguration conf : pipelineFragmentConfiguration.getOriginalStages()) {
            fetchStageDefinition(stageLibrary, conf, stageDefinitions, stageIcons);
          }
        });
      }

      List<StageDefinitionJson> stages = new ArrayList<>(BeanHelper.wrapStageDefinitions(stageDefinitions));
      definitions.setStages(stages);

      definitions.setStageIcons(stageIcons);

      List<PipelineFragmentDefinitionJson> pipelineFragment = new ArrayList<>(1);
      pipelineFragment.add(BeanHelper.wrapPipelineFragmentDefinition(stageLibrary.getPipelineFragment()));
      definitions.setPipelineFragment(pipelineFragment);

      List<PipelineRulesDefinitionJson> pipelineRules = new ArrayList<>(1);
      pipelineRules.add(BeanHelper.wrapPipelineRulesDefinition(stageLibrary.getPipelineRules()));
      definitions.setPipelineRules(pipelineRules);

      Map<Class, ServiceDefinition> serviceByClass = stageLibrary.getServiceDefinitions().stream()
          .collect(Collectors.toMap(ServiceDefinition::getProvides, Function.identity()));

      List<ServiceDefinition> pipelineServices = stageDefinitions.stream()
          .flatMap(stageDefinition -> stageDefinition.getServices().stream())
          .map(ServiceDependencyDefinition::getServiceClass)
          .distinct()
          .map(serviceClass -> serviceByClass.get(serviceClass))
          .filter(Objects::nonNull)
          .collect(Collectors.toList());

      definitions.setServices(BeanHelper.wrapServiceDefinitions(pipelineServices));

      pipelineFragmentEnvelope.setLibraryDefinitions(definitions);
    }

    return pipelineFragmentEnvelope;
  }

  private static void fetchStageDefinition(
      StageLibraryTask stageLibrary,
      StageConfiguration conf,
      List<StageDefinition> stageDefinitions,
      Map<String, String> stageIcons
  ) {
    String key = conf.getLibrary() + ":"  + conf.getStageName();
    if (!stageIcons.containsKey(key)) {
      StageDefinition stageDefinition = stageLibrary.getStage(conf.getLibrary(),
          conf.getStageName(), false);
      if (stageDefinition != null) {
        stageDefinitions.add(stageDefinition);
        String iconFile = stageDefinition.getIcon();
        if (iconFile != null && iconFile.trim().length() > 0) {
          try(InputStream icon = stageDefinition.getStageClassLoader().getResourceAsStream(iconFile)) {
            stageIcons.put(key, BaseEncoding.base64().encode(IOUtils.toByteArray(icon)));
          } catch (Exception e) {
            LOG.debug("Failed to convert stage icons to Base64 - " + e.getLocalizedMessage());
            stageIcons.put(key, null);
          }
        } else {
          stageIcons.put(key, null);
        }
      }
    }
  }

  public static List<ConfigDefinition> handleHideConfigs(Class<?> klass, List<ConfigDefinition> configs) {
    final HideConfigs hideConfigs = klass.getAnnotation(HideConfigs.class);
    if (hideConfigs != null) {
      final Set<String> hideConfigNames = new HashSet<>();
      if (hideConfigs.value() != null) {
        hideConfigNames.addAll(Arrays.asList(hideConfigs.value()));
      }
      if (!hideConfigNames.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Hiding configs from {}: {}", klass.getSimpleName(), hideConfigNames);
        }
        final List<ConfigDefinition> filteredConfigs = configs.stream()
            .filter(c -> !hideConfigNames.remove(c.getName()))
            .collect(Collectors.toList());
        if (!hideConfigNames.isEmpty()) {
          throw new IllegalStateException(String.format(
              "Config names marked hidden from PipelineConfig, but did not appear: %s",
              hideConfigNames
          ));
        }
        return filteredConfigs;
      }
    }
    return configs;
  }
}
