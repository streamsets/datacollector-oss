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
package com.streamsets.pipeline.stage.destination.http;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.http.logging.HttpConfigUpgraderUtil;
import com.streamsets.pipeline.stage.destination.lib.ResponseType;
import com.streamsets.pipeline.stage.util.tls.TlsConfigBeanUpgradeUtil;

import java.util.List;

public class HttpClientTargetUpgrader implements StageUpgrader {

    @Override
    public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs) throws
        StageException {
      switch(fromVersion) {
        case 1:
          TlsConfigBeanUpgradeUtil.upgradeHttpSslConfigBeanToTlsConfigBean(configs, "conf.client.");
          if (toVersion == 2) {
            break;
          }
          // fall through
        case 2:
          upgradeV2ToV3(configs);
          if (toVersion == 3) {
            break;
          }
          // fall-through to next version
        case 3:
          upgradeV3ToV4(configs);
          break;
        default:
          throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
      }
      return configs;
    }

  private void upgradeV2ToV3(List<Config> configs) {
    HttpConfigUpgraderUtil.addDefaultRequestLoggingConfigs(configs, "conf.client");
  }

  private void upgradeV3ToV4(List<Config> configs) {
    configs.add(new Config("conf.responseConf.sendResponseToOrigin", false));
    configs.add(new Config("conf.responseConf.responseType", ResponseType.SUCCESS_RECORDS));
  }

}
