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
package com.streamsets.pipeline.stage.origin.remote;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;

import java.util.ArrayList;
import java.util.List;

public class RemoteDownloadSourceUpgrader implements StageUpgrader {
  private static final String CONF = "conf";
  private static final String REMOTE_CONFIG = "remoteConfig";
  private static final Joiner joiner = Joiner.on(".");

  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance,
                              int fromVersion, int toVersion, List<Config> configs) throws StageException {
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
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV3ToV4(List<Config> configs) {
    // A bunch of the configs were moved from "conf.*" to "conf.remoteConfig.*"
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();
    for (Config config : configs) {
      switch (config.getName()) {
        case "conf.remoteAddress":
        case "conf.auth":
        case "conf.username":
        case "conf.password":
        case "conf.privateKeyProvider":
        case "conf.privateKey":
        case "conf.privateKeyPlainText":
        case "conf.privateKeyPassphrase":
        case "conf.userDirIsRoot":
        case "conf.strictHostChecking":
        case "conf.knownHosts":
          configsToAdd.add(new Config(
              joiner.join(CONF, REMOTE_CONFIG, config.getName().substring(5)),
              config.getValue()
          ));
          configsToRemove.add(config);
          break;
        default:
          // no op
      }
    }
    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);

  }

  private static void upgradeV2ToV3(List<Config> configs) {
    DataFormatUpgradeHelper.upgradeAvroParserWithSchemaRegistrySupport(configs);
  }

  private void upgradeV1ToV2(List<Config> configs) {
    String pollingInterval = joiner.join(CONF, "pollInterval");
    Config confToRemove = null;
    for (Config conf: configs) {
      if (conf.getName().equals(pollingInterval)) {
        confToRemove = conf;
      }
    }
    configs.remove(confToRemove);
  }
}
