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
package com.streamsets.pipeline.stage.destination.cassandra;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;

@GenerateResourceBundle
@StageDef(
    version = 5,
    label = "Cassandra",
    description = "Writes data to Cassandra",
    icon = "cassandra.png",
    upgrader = CassandraTargetUpgrader.class,
    onlineHelpRefUrl ="index.html?contextID=task_t1d_z3l_sr"
)
@ConfigGroups(value = Groups.class)
public class CassandraDTarget extends DTarget {

  @ConfigDefBean
  public CassandraTargetConfig conf = new CassandraTargetConfig();

  @Override
  protected Target createTarget() {
    return new CassandraTarget(conf);
  }
}
