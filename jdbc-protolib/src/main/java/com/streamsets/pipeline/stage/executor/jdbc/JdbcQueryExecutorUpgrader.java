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
package com.streamsets.pipeline.stage.executor.jdbc;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.JdbcBaseUpgrader;

import java.util.List;

/** {@inheritDoc} */
public class JdbcQueryExecutorUpgrader extends JdbcBaseUpgrader{

  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1toV2(configs);
        if (toVersion == 2) {
          break;
        }
        // fall through
      case 2:
        upgradeV2toV3(configs);
        break;
        // fall through

// ALEX

        //TODO when we use this class call JdbcBaseUpgrader.removeDriverClassNameAndTestQuery();

      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1toV2(List<Config> configs) {
    configs.add(new Config("config.batchCommit", false));
  }

  private void upgradeV2toV3(List<Config> configs) {
    configs.add(new Config("config.parallel", false));
  }
}
