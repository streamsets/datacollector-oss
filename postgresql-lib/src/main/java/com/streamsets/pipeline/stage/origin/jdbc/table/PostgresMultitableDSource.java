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

package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;
import com.streamsets.pipeline.lib.jdbc.EncryptionGroups;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.stage.config.PostgresHikariPoolConfigBean;

@StageDef(version = 1,
    label = "PostgreSQL Multitable Consumer",
    description = "Reads data from PostgreSQL using table names",
    icon = "com_streamsets_pipeline_stage_origin_jdbc_table_PostgresMultitableDSource.svg",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    resetOffset = true,
    producesEvents = true,
    upgraderDef = "upgrader/PostgresMultitableDSource.yaml",
    onlineHelpRefUrl = "index.html?contextID=task_vr5_5hv_33b",
    services = @ServiceDependency(service = SshTunnelService.class))
@ConfigGroups(value = EncryptionGroups.class)
@GenerateResourceBundle
@HideConfigs({
    "commonSourceConfigBean.allowLateTable",
    "commonSourceConfigBean.enableSchemaChanges",
    "commonSourceConfigBean.txnWindow",
})
public class PostgresMultitableDSource extends TableJdbcDSource {

  @ConfigDefBean
  public PostgresHikariPoolConfigBean hikariConfigBean;

  @Override
  protected HikariPoolConfigBean getHikariConfigBean() {
    return hikariConfigBean;
  }

}
