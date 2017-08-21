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
package com.streamsets.pipeline.stage.processor.splitter;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.List;

public class SplitterProcessorUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();

    String separator = "";

    for (Config config : configs) {
      switch (config.getName()) {
        case "separator":
          configsToRemove.add(config);
          separator = config.getValue().toString();
          break;
      }
    }

    configs.removeAll(configsToRemove);

    // In order to preserve forwards-compatibility, we need to escape any separator character
    // that was previously a regex metacharacter (one of .$|()[{^?*+\ ). For simplicity, we use a
    // regex matcher to determine if the character is a metacharacter (with everything escaped --
    // thus the ungodly number of backslashes). In the case of a space character, the UI doesn't
    // play nicely with spaces, so we replace it with a character class containing the space character.
    if (separator.matches("[\\.\\$\\|\\(\\)\\[\\{\\^\\?\\*\\+\\\\]")) {
      separator = "\\" + separator;
    } else if (separator.equals(" ")) {
      separator = "[ ]";
    }

    configs.add(new Config("separator", separator));
  }
}
