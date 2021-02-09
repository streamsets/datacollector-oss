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

package com.streamsets.pipeline.stage.pubsub.origin;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;
import com.streamsets.pipeline.stage.pubsub.lib.Groups;

@StageDef(
    version = 4,
    label = "Google Pub Sub Subscriber",
    description = "Consumes messages from a Google Pub/Sub subscription",
    icon = "pubsub.png",
    recordsByRef = true,
    execution = ExecutionMode.STANDALONE,
    upgraderDef = "upgrader/PubSubDSource.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_jvp_f5l_r1b"
)
@ConfigGroups(Groups.class)
public class PubSubDSource extends DPushSource {
  @ConfigDefBean
  public PubSubSourceConfig conf = new PubSubSourceConfig();

  @Override
  protected PushSource createPushSource() {
    return new PubSubSource(conf);
  }
}
