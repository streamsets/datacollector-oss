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

package com.streamsets.pipeline.stage.origin.jdbc;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;
import com.streamsets.pipeline.lib.event.NoMoreDataEvent;
import com.streamsets.pipeline.lib.jdbc.EncryptionGroups;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.stage.config.MySQLHikariPoolConfigBean;


@StageDef(version = 1,
    label = "MySQL Query Consumer",
    description = "Reads data from MySQL using a query",
    icon = "com_streamsets_pipeline_stage_origin_jdbc_MySQLDSource.svg",
    execution = ExecutionMode.STANDALONE,
    upgraderDef = "upgrader/MySQLDSource.yaml",
    recordsByRef = true,
    resetOffset = true,
    producesEvents = true,
    eventDefs = {NoMoreDataEvent.class},
    onlineHelpRefUrl = "index.html?contextID=task_nz4_h2v_33b",
    services = @ServiceDependency(service = SshTunnelService.class))
@GenerateResourceBundle
@HideConfigs({
    "commonSourceConfigBean.allowLateTable",
    "commonSourceConfigBean.enableSchemaChanges",
    "commonSourceConfigBean.queriesPerSecond",
    "commonSourceConfigBean.txnWindow",
})
@ConfigGroups(value = EncryptionGroups.class)
public class MySQLDSource extends JdbcDSource {
  @ConfigDefBean
  public MySQLHikariPoolConfigBean hikariConfigBean;

  @Override
  protected HikariPoolConfigBean getHikariConfigBean() {
    return hikariConfigBean;
  }
}
