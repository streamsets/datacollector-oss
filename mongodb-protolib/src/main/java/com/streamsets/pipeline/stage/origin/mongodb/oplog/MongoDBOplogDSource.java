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
package com.streamsets.pipeline.stage.origin.mongodb.oplog;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.stage.origin.mongodb.MongoDBDSource;

@StageDef(
    version = 1,
    label = "MongoDB Oplog",
    description = "Reads OpLog records from MongoDB",
    icon="mongodb.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    onlineHelpRefUrl = "index.html#Origins/MongoDBOplog.html#task_qj5_drw_4y",
    upgrader = MongoDBOplogSourceUpgrader.class,
    resetOffset = true
)
@GenerateResourceBundle
@HideConfigs(
    {
        "configBean.isCapped",
        "configBean.initialOffset",
        "configBean.offsetField",
        "configBean.mongoConfig.database",
        "configBean.offsetType",
    }
)
public class MongoDBOplogDSource extends MongoDBDSource {

  @ConfigDefBean()
  public MongoDBOplogSourceConfigBean mongoDBOplogSourceConfigBean;

  @Override
  protected Source createSource() {
    //Oplog is stored in Local database
    configBean.mongoConfig.database = "local";
    //Oplog collection is capped, but we don't actually use this
    configBean.isCapped = true;
    return new MongoDBOplogSource(configBean, mongoDBOplogSourceConfigBean);
  }
}
