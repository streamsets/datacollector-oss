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
package com.streamsets.pipeline.stage.origin.jms;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JmsSourceUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(List<Config> configs, Context context) throws StageException {
    switch(context.getFromVersion()) {
      case 1:
        upgradeV1ToV2(configs);
        if (context.getToVersion() == 2) {
          break;
        }
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        if (context.getToVersion() == 3) {
          break;
        }
        // fall through
      case 3:
        upgradeV3ToV4(configs);
        if (context.getToVersion() == 4) {
          break;
        }
        // fall through
      case 4:
        if (context.getToVersion() == 5) {
          break;
        }
        upgradeV4ToV5(configs);
      // fall through
      case 5:
        upgradeV5ToV6(configs, context);
        if (context.getToVersion() == 6) {
          break;
        }
        // fall through
      case 6:
        upgradeV6ToV7(configs);
        if (context.getToVersion() == 7) {
          break;
        }
        // fall through
      case 7:
        upgradeV7toV8(configs, context);
        if (context.getToVersion() == 8) {
          break;
        }
        // fall through
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", context.getFromVersion()));
    }
    return configs;
  }

  private static void upgradeV4ToV5(List<Config> configs) {
    DataFormatUpgradeHelper.upgradeAvroParserWithSchemaRegistrySupport(configs);
}

  private static void upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config("dataFormatConfig.compression", "NONE"));
    configs.add(new Config("dataFormatConfig.useCustomDelimiter", false));
    configs.add(new Config("dataFormatConfig.filePatternInArchive", "*"));
  }
  private static void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("jmsConfig.destinationType", "UNKNOWN"));
    configs.add(new Config("dataFormatConfig.csvSkipStartLines", 0));
  }

  private static void upgradeV3ToV4(List<Config> configs) {
    // This looks weird, but it's correct. Despite the fact that contextProperties
    // is a MAP config, the UI renders it as a list of maps, so new ArrayList gets
    // us what we want.
    configs.add(new Config("jmsConfig.contextProperties", new ArrayList<>()));
  }

  // Transition to services
  private static void upgradeV5ToV6(List<Config> configs, Context context) {
    List<Config> dataFormatConfigs = configs.stream()
      .filter(c -> c.getName().startsWith("dataFormat"))
      .collect(Collectors.toList());

    // Remove those configs
    configs.removeAll(dataFormatConfigs);

    // There is an interesting history with compression - at some point (version 2), we explicitly added it, then
    // we have hidden it. So this config might or might not exists, depending on the version in which the pipeline
    // was created. However the service is expecting it and thus we need to ensure that it's there.
    if(dataFormatConfigs.stream().noneMatch(c -> "dataFormatConfig.compression".equals(c.getName()))) {
      dataFormatConfigs.add(new Config("dataFormatConfig.compression", "NONE"));
    }

    // And finally register new service
    context.registerService(DataFormatParserService.class, dataFormatConfigs);
  }

  private static void upgradeV6ToV7(List<Config> configs) {
    configs.add(new Config("jmsConfig.useClientID", false));
    configs.add(new Config("jmsConfig.clientID", null));
    configs.add(new Config("jmsConfig.durableSubscription", false));
    configs.add(new Config("jmsConfig.durableSubscriptionName", null));
  }

  /**
   * Adding connection catalog.
   */
  private void upgradeV7toV8(List<Config> configs, Context context) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case "jmsConfig.initialContextFactory":
          configsToRemove.add(config);
          configsToAdd.add(new Config("jmsConfig.connection.initialContextFactory", config.getValue()));
          break;
        case "jmsConfig.connectionFactory":
          configsToRemove.add(config);
          configsToAdd.add(new Config("jmsConfig.connection.connectionFactory", config.getValue()));
          break;
        case "jmsConfig.providerURL":
          configsToRemove.add(config);
          configsToAdd.add(new Config("jmsConfig.connection.providerURL", config.getValue()));
          break;
        case "credentialsConfig.useCredentials":
          configsToRemove.add(config);
          configsToAdd.add(new Config("jmsConfig.connection.useCredentials", config.getValue()));
          break;
        case "credentialsConfig.username":
          configsToRemove.add(config);
          configsToAdd.add(new Config("jmsConfig.connection.username", config.getValue()));
          break;
        case "credentialsConfig.password":
          configsToRemove.add(config);
          configsToAdd.add(new Config("jmsConfig.connection.password", config.getValue()));
          break;
        default:
          break;
      }
    }

    configs.add(new Config("jmsConfig.connection.additionalSecurityProps", null));

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }
}
