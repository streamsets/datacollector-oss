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
package com.streamsets.pipeline.stage.origin.http;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DSource;

@StageDef(
    version = 19,
    label = "HTTP Client",
    description = "Uses an HTTP client to read records from an URL.",
    icon = "httpclient.png",
    execution = {ExecutionMode.STANDALONE, ExecutionMode.EDGE},
    resetOffset = true,
    // Must avoid using the records by reference otherwise we will fall into ESC-999 when using the "KeepAllFields"
    // config and cloning the original record got by the parser.
    recordsByRef = false,
    upgrader = HttpClientSourceUpgrader.class,
    upgraderDef = "upgrader/HttpClientDSource.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_akl_rkz_5r"
)
@HideConfigs(value = {
    "conf.client.numThreads"
})
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class HttpClientDSource extends DSource {

  @ConfigDefBean
  public HttpClientConfigBean conf;

  @Override
  protected Source createSource() {
    return new HttpClientSource(conf);
  }
}
