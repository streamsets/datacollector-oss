/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisBaseUpgrader;

import java.util.List;

import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.KINESIS_CONFIG_BEAN;

public class KinesisSourceUpgrader extends KinesisBaseUpgrader {

  @Override
  public List<Config> upgrade(
      String library,
      String stageName,
      String stageInstance,
      int fromVersion,
      int toVersion,
      List<Config> configs
  ) throws StageException {
    switch (fromVersion) {
      case 1:
        upgradeV1toV2(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1toV2(List<Config> configs) {
    upgradeToCommonConfigBeanV1(configs, KINESIS_CONFIG_BEAN);
    upgradeToConsumerConfigBeanV1(configs);
  }

  private void upgradeToConsumerConfigBeanV1(List<Config> configs) {
    for (Config config : configs) {
      // Migrate existing configs that were moved into the Kinesis Consumer config bean
      switch (config.getName()) {
        case "applicationName":
          // fall through
        case "maxBatchSize":
          // fall through
        case "idleTimeBetweenReads":
          // fall through
        case "maxWaitTime":
          // fall through
        case "previewWaitTime":
          moveConfigToBean(config, KINESIS_CONFIG_BEAN);
          break;
        default:
          // no-op
      }
    }
    commitMove(configs);

    configs.add(new Config(KINESIS_CONFIG_BEAN + ".initialPositionInStream", InitialPositionInStream.LATEST));
  }
}
