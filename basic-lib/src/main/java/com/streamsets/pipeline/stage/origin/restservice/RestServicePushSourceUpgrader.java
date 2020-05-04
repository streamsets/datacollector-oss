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
package com.streamsets.pipeline.stage.origin.restservice;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.UpgraderUtils;
import com.streamsets.pipeline.lib.tls.KeyStoreType;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestServicePushSourceUpgrader implements StageUpgrader {

  @Override
  public List<Config> upgrade(List<Config> configs, StageUpgrader.Context context) throws StageException {
    switch(context.getFromVersion()) {
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
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", context.getFromVersion()));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config("httpConfigs.tlsConfigBean.trustStoreFilePath", ""));
    configs.add(new Config("httpConfigs.tlsConfigBean.trustStoreType", KeyStoreType.JKS));
    configs.add(new Config("httpConfigs.tlsConfigBean.trustStorePassword", ""));
    configs.add(
        new Config("httpConfigs.tlsConfigBean.trustStoreAlgorithm", TlsConfigBean.DEFAULT_KEY_MANAGER_ALGORITHM)
    );
    configs.add(new Config("httpConfigs.needClientAuth", false));
  }

  private void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("responseConfig.sendRawResponse", false));
  }

  private void upgradeV3ToV4(List<Config> configs) {
    configs.add(new Config("dataFormatConfig.preserveRootElement", false));
  }

  private void upgradeV4ToV5(List<Config> configs) {
    Config appIdConfig = UpgraderUtils.getAndRemoveConfigWithName(configs, "httpConfigs.appId");
    if (appIdConfig != null) {
      List<Map<String,Object>> credentialValueBeanList = new ArrayList<>();
      Map<String,Object> mapCredentialValueBean = new HashMap<>();
      mapCredentialValueBean.put("appId",appIdConfig.getValue());
      credentialValueBeanList.add(mapCredentialValueBean);
      configs.add(new Config("httpConfigs.appIds", credentialValueBeanList));
    }
  }

}
