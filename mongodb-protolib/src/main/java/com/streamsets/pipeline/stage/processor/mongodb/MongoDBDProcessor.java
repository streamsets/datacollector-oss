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

package com.streamsets.pipeline.stage.processor.mongodb;

import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.stage.common.mongodb.Groups;

@StageDef(
        version = 1,
        label = "MongoDB Lookup",
        description = "Performs KV lookups to enrich records",
        icon = "mongodb.png",
        privateClassLoader = true,
        upgraderDef = "upgrader</MongoDBDProcessor.yaml",
        onlineHelpRefUrl ="index.html?contextID=task_yt1_w4w_2fb"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
@HideConfigs(
        "configBean.cacheConfig.retryOnCacheMiss"
)
public class MongoDBDProcessor extends DProcessor  {
  @ConfigDefBean(groups = {"MONGODB", "LOOKUP", "CREDENTIALS", "ADVANCED"})
  public MongoDBProcessorConfigBean configBean;

  @Override
  protected Processor createProcessor() {
    return new MongoDBProcessor(configBean);
  }
}
