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
package com.streamsets.pipeline.stage.destination.kudu;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;

@GenerateResourceBundle
@StageDef(
    version = 5,
    label = "Kudu",
    description = "Writes data to Kudu",
    icon = "kudu.png",
    privateClassLoader = true,
    onlineHelpRefUrl ="index.html?contextID=task_c4x_tmh_4v",
    upgrader = KuduTargetUpgrader.class
)
@ConfigGroups(Groups.class)
public class KuduDTarget extends DTarget {

  @ConfigDefBean
  public KuduConfigBean kuduConfigBean;

  @Override
  protected Target createTarget() {
    return new KuduTarget(kuduConfigBean);
  }

}
