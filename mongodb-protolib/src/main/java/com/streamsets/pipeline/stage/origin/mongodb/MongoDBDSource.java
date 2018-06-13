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
package com.streamsets.pipeline.stage.origin.mongodb;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DSource;
import com.streamsets.pipeline.stage.common.mongodb.Groups;

@StageDef(
    version = 3,
    label = "MongoDB",
    description = "Reads records from a MongoDB collection",
    icon="mongodb.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    producesEvents = true,
    onlineHelpRefUrl ="index.html?contextID=task_mdf_2rs_ns",
    upgrader = MongoDBSourceUpgrader.class,
    resetOffset = true
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class MongoDBDSource extends DSource {

  @ConfigDefBean()
  public MongoSourceConfigBean configBean;

  @Override
  protected Source createSource() {
    return new MongoDBSource(configBean);
  }
}
