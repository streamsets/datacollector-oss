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
package com.streamsets.pipeline.stage.destination.mongodb;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.stage.common.mongodb.Groups;

@StageDef(
    version = 3,
    label = "MongoDB",
    description = "Writes data to MongoDB",
    icon="mongodb.png",
    onlineHelpRefUrl = "index.html#Destinations/MongoDB.html#task_mrc_k5n_4v",
    upgrader = MongoDBTargetUpgrader.class
)

@ConfigGroups(value = Groups.class)
@HideConfigs("configBean.mongoConfig.isSingleMode")
@GenerateResourceBundle
public class MongoDBDTarget extends DTarget {

  @ConfigDefBean()
  public MongoTargetConfigBean configBean;

  @Override
  protected Target createTarget() {
    return new MongoDBTarget(configBean);
  }
}
