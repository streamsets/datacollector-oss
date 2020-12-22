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
package com.streamsets.pipeline.stage.destination.elasticsearch;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ElasticsearchDTargetUpgrader implements StageUpgrader {
  private static final String DEFAULT_HTTP_URI = "hostname";

  static final String OLD_CONFIG_PREFIX = "elasticSearchConfigBean.";
  static final String OLD_SECURITY_PREFIX = "elasticSearchConfigBean.securityConfigBean.";

  static final String CURRENT_CONFIG_PREFIX = "elasticSearchConfig.";
  static final String CURRENT_SECURITY_CONFIG_PREFIX = "securityConfig.";

  @Override
  public List<Config> upgrade(
      String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs
  ) throws StageException {
    switch (fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        if (toVersion == 2) {
          break;
        }
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        if (toVersion == 3) {
          break;
        }
        // fall through
      case 3:
        upgradeV3ToV4(configs);
        if (toVersion == 4) {
          break;
        }
        // fall through
      case 4:
        upgradeV4ToV5(configs);
        if (toVersion == 5) {
          break;
        }
        // fall through
      case 5:
        upgradeV5ToV6(configs);
        if (toVersion == 6) {
          break;
        }
        // fall through
      case 6:
        configs = upgradeV6ToV7(configs);
        if (toVersion == 7) {
          break;
        }
        // fall through
      case 7:
        upgradeV7ToV8(configs);
        if (toVersion == 8) {
          break;
        }
        // fall through
      case 8:
        upgradeV8ToV9(configs);
        if (toVersion == 9) {
        break;
      }
      // fall through
      case 9:
        upgradeV9toV10(configs);
        if (toVersion == 10) {
          break;
        }
      case 10:
      case 11:
        // handled by YAML upgrader
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private static void upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config("timeDriver", "${time:now()}"));
    configs.add(new Config("timeZoneID", "UTC"));
  }

  @SuppressWarnings("unchecked")
  private void upgradeV2ToV3(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case "clusterName":
        case "uris":
        case "timeDriver":
        case "timeZoneID":
        case "indexTemplate":
        case "typeTemplate":
        case "docIdTemplate":
        case "charset":
          configsToAdd.add(new Config(OLD_CONFIG_PREFIX + config.getName(), config.getValue()));
          configsToRemove.add(config);
          break;
        case "configs":
          // Remove client.transport.sniff from the additional configs when loading an old pipeline.
          // This config is disabled by default, and it should be enabled only when the user explicitly
          // checks a new checkbox in the UI.
          List<Map<String, String>> keyValues = (List<Map<String, String>>) config.getValue();
          Map<String, String> clientSniffKeyValue = null;
          for (Map<String, String> keyValue : keyValues) {
            for (Map.Entry entry : keyValue.entrySet()) {
              if (entry.getValue().equals("client.transport.sniff")) {
                clientSniffKeyValue = keyValue;
              }
            }
          }
          if (clientSniffKeyValue != null) {
            keyValues.remove(clientSniffKeyValue);
          }
          configsToAdd.add(new Config(OLD_CONFIG_PREFIX + config.getName(), keyValues));
          configsToRemove.add(config);
          break;
        default:
          // no op
      }
    }
    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }

  private static void upgradeV3ToV4(List<Config> configs) {
    configs.add(new Config(OLD_CONFIG_PREFIX + "httpUri", DEFAULT_HTTP_URI));
  }

  private static void upgradeV4ToV5(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    // Rename useFound to useElasticCloud.
    for (Config config : configs) {
      if (config.getName().equals(OLD_CONFIG_PREFIX + "useFound")) {
        configsToAdd.add(new Config(config.getName().replace("useFound", "useElasticCloud"), config.getValue()));
        configsToRemove.add(config);
        break;
      }
    }

    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }

  private static void upgradeV5ToV6(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      // Rename shieldConfigBean to securityConfig.
      if (config.getName().equals(OLD_CONFIG_PREFIX + "shieldConfigBean.shieldUser")) {
        configsToAdd.add(new Config(config.getName().replace("shield", "security"), config.getValue()));
        configsToRemove.add(config);
      }
      // Remove shieldTransportSsl.
      if (config.getName().equals(OLD_CONFIG_PREFIX + "shieldConfigBean.shieldTransportSsl")) {
        configsToRemove.add(config);
      }
      // Remove sslKeystorePath.
      if (config.getName().equals(OLD_CONFIG_PREFIX + "shieldConfigBean.sslKeystorePath")) {
        configsToRemove.add(config);
      }
      // Remove sslKeystorePassword.
      if (config.getName().equals(OLD_CONFIG_PREFIX + "shieldConfigBean.sslKeystorePassword")) {
        configsToRemove.add(config);
      }
      // Rename shieldConfigBean to securityConfig.
      if (config.getName().equals(OLD_CONFIG_PREFIX + "shieldConfigBean.sslTruststorePath")) {
        configsToAdd.add(new Config(config.getName().replace("shield", "security"), config.getValue()));
        configsToRemove.add(config);
      }
      // Rename shieldConfigBean to securityConfig.
      if (config.getName().equals(OLD_CONFIG_PREFIX + "shieldConfigBean.sslTruststorePassword")) {
        configsToAdd.add(new Config(config.getName().replace("shield", "security"), config.getValue()));
        configsToRemove.add(config);
      }
      // Remove clusterName.
      if (config.getName().equals(OLD_CONFIG_PREFIX + "clusterName")) {
        configsToRemove.add(config);
      }
      // Remove uris.
      if (config.getName().equals(OLD_CONFIG_PREFIX + "uris")) {
        configsToRemove.add(config);
      }
      // Rename httpUri to httpUris.
      if (config.getName().equals(OLD_CONFIG_PREFIX + "httpUri")) {
        String configValue = Optional.ofNullable((String) config.getValue())
            .orElse(DEFAULT_HTTP_URI);
        if (!configValue.isEmpty() && !configValue.equals(DEFAULT_HTTP_URI)) {
          configsToAdd.add(new Config(OLD_CONFIG_PREFIX + "httpUris", Arrays.asList(configValue)));
        }
        configsToRemove.add(config);
      }
      // Rename useShield to useSecurity.
      if (config.getName().equals(OLD_CONFIG_PREFIX + "useShield")) {
        configsToAdd.add(new Config(config.getName().replace("Shield", "Security"), config.getValue()));
        configsToRemove.add(config);
      }
      // Remove useElasticCloud.
      if (config.getName().equals(OLD_CONFIG_PREFIX + "useElasticCloud")) {
        configsToRemove.add(config);
      }
      // Rename configs to params.
      if (config.getName().equals(OLD_CONFIG_PREFIX + "configs")) {
        configsToAdd.add(new Config(config.getName().replace("configs", "params"), config.getValue()));
        configsToRemove.add(config);
      }
      // Remove upsert.
      if (config.getName().equals(OLD_CONFIG_PREFIX + "upsert")) {
        if ((Boolean) config.getValue()) {
          configsToAdd.add(new Config(OLD_CONFIG_PREFIX + "defaultOperation", "INDEX"));
        } else {
          configsToAdd.add(new Config(OLD_CONFIG_PREFIX + "defaultOperation", "CREATE"));
        }
        configsToRemove.add(config);
      }
    }

    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }

  private List<Config> upgradeV6ToV7(List<Config> configs) {
   return configs.stream()
      // Rename of beans
      .map(config -> {
        if (config.getName().startsWith(OLD_SECURITY_PREFIX)) {
          return new Config(
            config.getName().replace(OLD_SECURITY_PREFIX, CURRENT_CONFIG_PREFIX + "securityConfig."),
            config.getValue()
          );
        } else if (config.getName().startsWith(OLD_CONFIG_PREFIX)) {
          return new Config(
              config.getName().replace(OLD_CONFIG_PREFIX, CURRENT_CONFIG_PREFIX),
              config.getValue()
          );
        } else {
          return config;
        }
      })
      // Fixing case of sslTrustStore
     .map(config -> {
       if(config.getName().contains("sslTruststore")) {
         return new Config(
           config.getName().replace("sslTruststore", "sslTrustStore"),
           config.getValue()
         );
       } else {
         return config;
       }
     })
     // And we're done
     .collect(Collectors.toList());
  }

  private void upgradeV7ToV8(List<Config> configs) {
    configs.add(new Config(CURRENT_CONFIG_PREFIX + "parentIdTemplate", ""));
    configs.add(new Config(CURRENT_CONFIG_PREFIX + "routingTemplate", ""));
  }

  private void upgradeV8ToV9(List<Config> configs) {
    configs.add(new Config(CURRENT_CONFIG_PREFIX + CURRENT_SECURITY_CONFIG_PREFIX + "securityMode", "BASIC"));
    configs.add(new Config(CURRENT_CONFIG_PREFIX + CURRENT_SECURITY_CONFIG_PREFIX + "awsRegion", "US_EAST_2"));
  }

  private void upgradeV9toV10(List<Config> configs) {
    configs.add(new Config(CURRENT_CONFIG_PREFIX + "rawAdditionalProperties", "{\n}"));
  }

}
