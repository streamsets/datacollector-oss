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
package com.streamsets.datacollector.creation;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.PipelineState;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Collections;
import java.util.List;

public class PipelineConfigUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion,
                              List<Config> configs) throws StageException {
    switch(fromVersion) {
      case 0:
        // nothing to do from 0 to 1
      case 1:
        upgradeV1ToV2(configs);
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        // fall through
      case 3:
        upgradeV3ToV4(configs);
        // fall through
      case 4:
        upgradeV4ToV5(configs);
        // fall through
      case 5:
        upgradeV5ToV6(configs);
        // fall through
      case 6:
        upgradeV6ToV7(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    boolean found = false;
    for (Config config : configs) {
      if (config.getName().equals("executionMode")) {
        found = true;
      }
    }

    if(!found) {
      configs.add(new Config("executionMode", ExecutionMode.STANDALONE));
    }
  }

  private void upgradeV3ToV4(List<Config> configs) {
    boolean found = false;
    int index = 0;
    String sourceName = null;
    for (int i = 0; i < configs.size(); i++) {
      Config config = configs.get(i);
      if (config.getName().equals("executionMode")) {
        if (config.getValue().equals("CLUSTER")) {
          found = true;
          index = i;
        }
      } else if (config.getName().equals("sourceName")) {
        sourceName = config.getValue().toString();
      }
    }
    if (found) {
      configs.remove(index);
      Utils.checkNotNull(sourceName, "Source stage name cannot be null");
      configs.add(new Config("executionMode", (sourceName.contains("ClusterHdfsDSource")) ? ExecutionMode.CLUSTER_BATCH
        : ExecutionMode.CLUSTER_YARN_STREAMING));
    }
  }

  private void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("shouldRetry", false));
    configs.add(new Config("retryAttempts", -1));
    configs.add(new Config("notifyOnStates",
      ImmutableList.of(PipelineState.RUN_ERROR, PipelineState.STOPPED, PipelineState.FINISHED)));
    configs.add(new Config("emailIDs", Collections.EMPTY_LIST));
  }

  private void upgradeV4ToV5(List<Config> configs) {
    configs.add(new Config("statsAggregatorStage", null));
  }

  private void upgradeV5ToV6(List<Config> configs) {
    configs.add(new Config("webhookConfigs", Collections.EMPTY_LIST));
  }

  private void upgradeV6ToV7(List<Config> configs) {
    configs.add(new Config("workerCount", 0));
  }
}
