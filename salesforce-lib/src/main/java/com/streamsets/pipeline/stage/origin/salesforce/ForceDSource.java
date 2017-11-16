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
package com.streamsets.pipeline.stage.origin.salesforce;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DSource;
import com.streamsets.pipeline.lib.salesforce.ForceSourceConfigBean;

@StageDef(
    version = 2,
    label = "Salesforce",
    description = "Reads data from Salesforce",
    icon = "salesforce.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    resetOffset = true,
    producesEvents = true,
    upgrader = ForceSourceUpgrader.class,
    onlineHelpRefUrl = "index.html#Origins/Salesforce.html#task_h1n_bs3_rx"
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
@HideConfigs(
    value = {
        "forceConfig.useCompression", "forceConfig.showTrace"
    }
)
public class ForceDSource extends DSource {
  @ConfigDefBean
  public ForceSourceConfigBean forceConfig;

  @Override
  protected Source createSource() {
    return new ForceSource(forceConfig);
  }
}
