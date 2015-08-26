/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
      case 1:
        upgradeV1ToV2(configs);
      case 2:
        upgradeV2ToV3(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config("executionMode", ExecutionMode.STANDALONE));
  }

  private void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("shouldRetry", false));
    configs.add(new Config("retryAttempts", -1));
    configs.add(new Config("notifyOnStates",
      ImmutableList.of(PipelineState.RUN_ERROR, PipelineState.STOPPED, PipelineState.FINISHED)));
    configs.add(new Config("emailIDs", Collections.EMPTY_LIST));
  }

}
