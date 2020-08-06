/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.sqs;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;

import java.util.List;
import java.util.stream.Collectors;

public class SqsUpgrader implements StageUpgrader {

  @Override
  public List<Config> upgrade(List<Config> configs, Context context) throws StageException {
    switch(context.getFromVersion()) {
      case 1:
        upgradeV1ToV2(configs, context);
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", context.getFromVersion()));
    }
    return configs;
  }

  // Transition to services
  private static void upgradeV1ToV2(List<Config> configs, Context context) {
    List<Config> dataFormatConfigs = configs.stream()
      .filter(c -> c.getName().startsWith("sqsConfig.dataFormat"))
      .collect(Collectors.toList());

    // Remove those configs
    configs.removeAll(dataFormatConfigs);

    // Remove SQS specific prefix
    dataFormatConfigs = dataFormatConfigs.stream()
      .map(c -> new Config(c.getName().replace("sqsConfig.", ""), c.getValue()))
      .collect(Collectors.toList());

    // And finally register new service
    context.registerService(DataFormatParserService.class, dataFormatConfigs);
  }

  private static void upgradeV2ToV3(List<Config> configs) {
    String regionProperty = "sqsConfig.region";
    for (int i = 0; i < configs.size(); i++) {
      if (configs.get(i).getName().equals(regionProperty)) {
        if ("GovCloud".equals(configs.get(i).getValue())) {
          configs.set(i, new Config(regionProperty, AwsRegion.US_GOV_WEST_1.name()));
        }
      }
    }
  }

}
