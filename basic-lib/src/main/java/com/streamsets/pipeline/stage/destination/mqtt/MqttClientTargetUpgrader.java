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
package com.streamsets.pipeline.stage.destination.mqtt;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.origin.mqtt.MqttClientSourceUpgrader;
import com.streamsets.pipeline.stage.util.tls.TlsConfigBeanUpgradeUtil;

import java.util.List;

public class MqttClientTargetUpgrader implements StageUpgrader {
    @Override
    public List<Config> upgrade(List<Config> configs, Context context) throws
        StageException {
      switch(context.getFromVersion()) {
        case 1:
          TlsConfigBeanUpgradeUtil.upgradeHttpSslConfigBeanToTlsConfigBean(configs, "commonConf.");
          if (context.getToVersion() == 2) {
            break;
          }
          // fall through
        case 2:
          MqttClientSourceUpgrader.addCleanSessionFlag(configs);
          if (context.getToVersion() == 3) {
            break;
          }
          // fall through
        case 3:
          upgradeV3ToV4(configs);
          break;
        default:
          throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", context.getFromVersion()));
      }
      return configs;
    }

  private void upgradeV3ToV4(List<Config> configs) {
    configs.add(new Config("publisherConf.runtimeTopicResolution", false));
    configs.add(new Config("publisherConf.topicExpression", "${record:value('/topic')}"));
    configs.add(new Config("publisherConf.topicWhiteList", "*"));
  }
}
