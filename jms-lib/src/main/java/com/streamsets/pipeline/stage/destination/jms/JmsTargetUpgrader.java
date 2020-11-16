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
package com.streamsets.pipeline.stage.destination.jms;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.lib.jms.config.connection.SecurityPropertyBean;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JmsTargetUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(List<Config> configs, Context context) throws StageException {
    switch (context.getFromVersion()) {
      case 1:
        upgradeV1ToV2(configs, context);
        if (context.getToVersion() == 2) {
          break;
        }
        // fall through
      case 2:
        upgradeV2toV3(configs, context);
        if (context.getToVersion() == 3) {
          break;
        }
        // fall through
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", context.getFromVersion()));
    }

    return configs;
  }

  /**
   * Migrating to service for data format library.
   */
  private void upgradeV1ToV2(List<Config> configs, Context context) {
    List<Config> dataFormatConfigs = configs.stream()
      .filter(c -> c.getName().startsWith("dataFormat"))
      .collect(Collectors.toList());

    // Remove those configs
    configs.removeAll(dataFormatConfigs);

    // Provide proper prefix
    dataFormatConfigs = dataFormatConfigs.stream()
      .map(c -> new Config(c.getName().replace("dataFormatConfig.", "dataGeneratorFormatConfig."), c.getValue()))
      .collect(Collectors.toList());

    // And finally register new service
    context.registerService(DataFormatGeneratorService.class, dataFormatConfigs);
  }

  /**
   * Adding connection catalog.
   */
  private void upgradeV2toV3(List<Config> configs, Context context) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case "jmsTargetConfig.initialContextFactory":
          configsToRemove.add(config);
          configsToAdd.add(new Config("jmsTargetConfig.connection.initialContextFactory", config.getValue()));
          break;
        case "jmsTargetConfig.connectionFactory":
          configsToRemove.add(config);
          configsToAdd.add(new Config("jmsTargetConfig.connection.connectionFactory", config.getValue()));
          break;
        case "jmsTargetConfig.providerURL":
          configsToRemove.add(config);
          configsToAdd.add(new Config("jmsTargetConfig.connection.providerURL", config.getValue()));
          break;
        case "credentialsConfig.useCredentials":
          configsToRemove.add(config);
          configsToAdd.add(new Config("jmsTargetConfig.connection.useCredentials", config.getValue()));
          break;
        case "credentialsConfig.username":
          configsToRemove.add(config);
          configsToAdd.add(new Config("jmsTargetConfig.connection.username", config.getValue()));
          break;
        case "credentialsConfig.password":
          configsToRemove.add(config);
          configsToAdd.add(new Config("jmsTargetConfig.connection.password", config.getValue()));
          break;
        default:
          break;
      }
    }

    configs.add(new Config("jmsTargetConfig.connection.additionalSecurityProps", null));

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }
}
