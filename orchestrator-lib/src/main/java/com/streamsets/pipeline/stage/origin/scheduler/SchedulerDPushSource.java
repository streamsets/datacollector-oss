/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.scheduler;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;

@StageDef(
    version = 2,
    label = "Cron Scheduler",
    description = "Generates a record with the current datetime based on a cron expression",
    icon="scheduler.png",
    recordsByRef = true,
    execution = {ExecutionMode.STANDALONE},
    onlineHelpRefUrl ="index.html?contextID=task_hcz_x4r_2jb",
    upgraderDef = "upgrader/SchedulerDPushSource.yaml"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class SchedulerDPushSource extends DPushSource {

  @ConfigDefBean
  public SchedulerConfig conf;

  @Override
  protected PushSource createPushSource() {
    return new SchedulerPushSource(conf);
  }

}
