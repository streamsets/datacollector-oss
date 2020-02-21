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

package com.streamsets.pipeline.stage.destination;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;
import com.streamsets.pipeline.lib.jdbc.EncryptionGroups;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.stage.config.MySQLHikariPoolConfigBean;
import com.streamsets.pipeline.stage.destination.jdbc.JdbcDTarget;

@GenerateResourceBundle
@StageDef(version = 12,
    label = "MySQL Producer",
    description = "Writes data to MySQL",
    upgraderDef = "upgrader/MySQLDTarget.yaml",
    icon = "com_streamsets_pipeline_stage_destination_MySQLDTarget.svg",
    onlineHelpRefUrl = "index.html?contextID=task_t2j_ghv_33b",
    services = @ServiceDependency(service = SshTunnelService.class))
@HideConfigs(value = {
    "hikariConfigBean.readOnly", "hikariConfigBean.autoCommit", "schema"
})
@ConfigGroups(value = EncryptionGroups.class)
public class MySQLDTarget extends JdbcDTarget {

  @ConfigDefBean
  public MySQLHikariPoolConfigBean hikariConfigBean;

  @Override
  public String getSchema() {
    String[] splitConnectionString = hikariConfigBean.connectionString.split("/");
    return splitConnectionString[splitConnectionString.length - 1];
  }

  @Override
  protected HikariPoolConfigBean getHikariConfigBean() {
    return hikariConfigBean;
  }

}
