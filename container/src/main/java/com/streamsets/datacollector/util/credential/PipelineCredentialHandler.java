/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.util.credential;

import com.google.common.base.Strings;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ModelType;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineDefinition;
import com.streamsets.datacollector.config.ServiceConfiguration;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.ServiceDependencyDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ElUtil;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigDef;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * A handler which visits the credential field
 */
public class PipelineCredentialHandler {
  static final String CREDENTIAL_EL_TEMPLATE = "${credential:get('%s','%s','%s')}";
  static final String CREDENTIAL_EL_PREFIX_TEMPLATE = "${credential:get('%s',";

  private final StageLibraryTask stageLibraryTask;
  private CredentialConfigHandler credentialConfigHandler;

  private final Predicate<Pair<ConfigDefinition, Object>> credentialConfigPredicate = (
      configDefinitionConfigPair -> configDefinitionConfigPair.getLeft()
          .getType()
          .equals(ConfigDef.Type.CREDENTIAL) && configDefinitionConfigPair.getRight() != null
  );

  PipelineCredentialHandler(
      StageLibraryTask stageLibraryTask,
      CredentialConfigHandler credentialConfigHandler
  ) {
    this.stageLibraryTask = stageLibraryTask;
    this.credentialConfigHandler = credentialConfigHandler;
  }

  static String generateSecretName(String pipelineId, String stageId, String configPath) {
    //TODO: If there is no stage information
    String secretName = CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + pipelineId;
    if (!Strings.isNullOrEmpty(stageId)) {
      secretName = secretName + "/" + stageId;
    }
    if (!Strings.isNullOrEmpty(configPath)) {
      secretName = secretName + "__" + configPath;
    }
    return secretName;
  }

  /**
   * Returns a Pipeline Credentials Handler which will encrypt the credential fields in plain text
   * @param stageLibraryTask Stage Library Task
   * @param credentialStoresTask Credential Stores Task
   * @param configuration  Configuration
   * @return Pipeline Credential Handler
   */
  public static PipelineCredentialHandler getEncrypter(
      StageLibraryTask stageLibraryTask,
      CredentialStoresTask credentialStoresTask,
      Configuration configuration
  ) {
    return new PipelineCredentialHandler(
        stageLibraryTask,
        new EncryptingCredentialConfigHandler(credentialStoresTask, configuration)
    );
  }

  /**
   * Returns a Pipeline Credentials Handler which will decrypt the credential fields which were managed
   * @param stageLibraryTask Stage Library Task
   * @param credentialStoresTask Credential Stores Task
   * @param configuration  Configuration
   * @return Pipeline Credential Handler
   */
  public static PipelineCredentialHandler getDecrypter(
      StageLibraryTask stageLibraryTask,
      CredentialStoresTask credentialStoresTask,
      Configuration configuration
  ) {
    return new PipelineCredentialHandler(
        stageLibraryTask,
        new DecryptingCredentialConfigHandler(credentialStoresTask, configuration)
    );
  }

  /**
   * Returns a Pipeline Credentials Handler which can scrub the plain text credentials
   * @param stageLibraryTask Stage Library Task
   * @return Pipeline Credential Handler
   */
  public static PipelineCredentialHandler getPlainTextScrubber(StageLibraryTask stageLibraryTask) {
    return new PipelineCredentialHandler(stageLibraryTask, (configDefinitionValue, configContext) -> {
      if (!ElUtil.isElString(configDefinitionValue.getRight())) {
        return "";
      }
      return configDefinitionValue.getRight();
    });
  }

  /**
   * Handles the credential fields in pipeline configuration
   * @param pipelineConfiguration pipeline configuration
   */
  public void handlePipelineConfigCredentials(PipelineConfiguration pipelineConfiguration) {
    // pipeline configs
    PipelineDefinition pipelineDefinition = stageLibraryTask.getPipeline();

    ConfigContext context = new ConfigContext();
    context.setPipelineId(pipelineConfiguration.getPipelineId());

    List<Config> newConfigs = handleConfigCredentials(
        pipelineConfiguration.getConfiguration(),
        getMapOfConfigDefinitions(pipelineDefinition.getConfigDefinitions()),
        context.cloneContext()
    );

    pipelineConfiguration.getConfiguration().clear();
    pipelineConfiguration.getConfiguration().addAll(newConfigs);

    // stages configs
    pipelineConfiguration.getStages().forEach(s -> handleStageConfigCredentials(s, context.cloneContext()));

    // special stage - error stage
    if (pipelineConfiguration.getErrorStage() != null) {
      handleStageConfigCredentials(pipelineConfiguration.getErrorStage(), context.cloneContext());
    }

    // special stage - stats aggr stage
    if (pipelineConfiguration.getStatsAggregatorStage() != null) {
      handleStageConfigCredentials(pipelineConfiguration.getStatsAggregatorStage(), context.cloneContext());
    }

    // special stage - test origin
    if (pipelineConfiguration.getTestOriginStage() != null) {
      handleStageConfigCredentials(pipelineConfiguration.getTestOriginStage(), context.cloneContext());
    }

    // special stage - start event
    if (!pipelineConfiguration.getStartEventStages().isEmpty()) {
      handleStageConfigCredentials(pipelineConfiguration.getStartEventStages().get(0), context.cloneContext());
    }

    // special stage - stop event
    if (!pipelineConfiguration.getStopEventStages().isEmpty()) {
      handleStageConfigCredentials(pipelineConfiguration.getStopEventStages().get(0), context.cloneContext());
    }
  }


  private void handleStageConfigCredentials(StageConfiguration stageInstance, ConfigContext context) {
    context.setStageId(stageInstance.getInstanceName());
    StageDefinition stageDefinition = stageLibraryTask.getStage(
        stageInstance.getLibrary(),
        stageInstance.getStageName(),
        false
    );
    if (stageDefinition != null) {
      List<Config> newStageConfigs = handleConfigCredentials(
          stageInstance.getConfiguration(),
          getMapOfConfigDefinitions(stageDefinition.getConfigDefinitions()),
          context.cloneContext()
      );
      stageInstance.setConfig(newStageConfigs);

      // Handle Services
      if (CollectionUtils.isNotEmpty(stageDefinition.getServices())) {
        List<ServiceDefinition> serviceDefinitions = stageLibraryTask.getServiceDefinitions();
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
            List<Config> newServiceConfigs = handleConfigCredentials(
                serviceConfiguration.getConfiguration(),
                getMapOfConfigDefinitions(serviceDefinition.getConfigDefinitions()),
                context.cloneContext()
            );
            serviceConfiguration.setConfig(newServiceConfigs);
          }
        }
      }
    }
  }

  private List<Config> handleConfigCredentials(
      List<Config> configList,
      Map<String, ConfigDefinition> configDefinitionMap,
      ConfigContext context
  ) {
    Map<String, Config> replacementConfigMap = new HashMap<>();
    configList.forEach(config -> {
      if (configDefinitionMap.containsKey(config.getName())) {
        ConfigDefinition configDefinition = configDefinitionMap.get(config.getName());
        ConfigContext configContext = context.cloneContext();
        configContext.setConfigName(config.getName());
        if (credentialConfigPredicate.test(Pair.of(configDefinition, config.getValue()))) {
          replacementConfigMap.put(
              config.getName(),
              new Config(
                  config.getName(),
                  credentialConfigHandler.handleCredential(Pair.of(configDefinition, config.getValue()), configContext)
              )
          );
        } else if (configDefinition.getModel() != null &&
            configDefinition.getModel().getModelType().equals(ModelType.LIST_BEAN)) {
          Map<String, ConfigDefinition> listBeanConfigDefinitionMap =
              getMapOfConfigDefinitions(configDefinition.getModel().getConfigDefinitions());
          if (config.getValue() != null) {
            if (config.getValue() instanceof List) {
              // list of hash maps
              List<Map<String, Object>> maps = (List<Map<String, Object>>) config.getValue();
              for (int i=0; i < maps.size(); i++) {
                ConfigContext listContext = configContext.cloneContext();
                listContext.setConfigName(configContext.getConfigName() + "[" + i + "]");
                handleListBeanConfigCredentials(listBeanConfigDefinitionMap, maps.get(i), listContext);
              }
            } else if (config.getValue() instanceof Map) {
              Map<String, Object> listBeanConfigValue = (Map<String, Object>) config.getValue();
              handleListBeanConfigCredentials(listBeanConfigDefinitionMap, listBeanConfigValue, context.cloneContext());
            }
          }
        }
      }
    });
    return createWithNewConfigs(configList, replacementConfigMap);
  }

  private void handleListBeanConfigCredentials(
      Map<String, ConfigDefinition> listBeanConfigDefinitionMap,
      Map<String, Object> listBeanConfigValue,
      ConfigContext context
  ) {
    for (Map.Entry<String, Object> entry : listBeanConfigValue.entrySet()) {
      String configName = entry.getKey();
      Object value = entry.getValue();

      ConfigContext mapConfigContext = context.cloneContext();
      mapConfigContext.setConfigName(context.getConfigName() + "[" + configName + "]");

      ConfigDefinition listBeanConfigDefinition = listBeanConfigDefinitionMap.get(configName);

      if (credentialConfigPredicate.test(Pair.of(listBeanConfigDefinition, value))) {
        entry.setValue(credentialConfigHandler.handleCredential(Pair.of(listBeanConfigDefinition, value), mapConfigContext));
      }
    }
  }

  private Map<String, ConfigDefinition> getMapOfConfigDefinitions(List<ConfigDefinition> configDefinitions) {
    Map<String, ConfigDefinition> configDefinitionMap = new HashMap<>();
    configDefinitions.forEach(c -> configDefinitionMap.put(c.getName(), c));
    return configDefinitionMap;
  }

  private List<Config> createWithNewConfigs(List<Config> configs, Map<String, Config> replacementConfigMap) {
    List<Config> newConfigurations = new ArrayList<>();
    for (Config candidate : configs) {
      newConfigurations.add(replacementConfigMap.getOrDefault(candidate.getName(), candidate));
    }
    return newConfigurations;
  }
}
