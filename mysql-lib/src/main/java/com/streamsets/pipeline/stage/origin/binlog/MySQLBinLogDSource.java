/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.binlog;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;
import com.streamsets.pipeline.stage.config.MySQLBinLogGroups;
import com.streamsets.pipeline.stage.config.MysqlBinLogSourceConfig;
import com.streamsets.pipeline.stage.origin.mysql.binlog.MysqlBinLogSource;

@StageDef(
    version = 1,
    label = "MySQL Binary Log",
    description = "Reads MySQL binary logs to generate change data capture records",
    icon = "com_streamsets_pipeline_stage_origin_jdbc_MySQLBinLogDSource.svg",
    execution = ExecutionMode.STANDALONE,
    resetOffset = true,
    recordsByRef = true,
    upgraderDef = "upgrader/MySqlBinaryLogSourceUpgrader.yaml",
    onlineHelpRefUrl = "index.html?contextID=task_qbt_kyh_xx",
    services = {@ServiceDependency(service = SshTunnelService.class)}
)
@HideConfigs(value = {
    "config.connectionString",
    "config.useCredentials",
    "config.driverProperties",
    "config.minIdle",
    "config.idleTimeout",
    "config.maxLifetime",
    "config.autoCommit",
    "config.readOnly",
    "config.initialQuery",
    "config.maxLifetime",
    "config.transactionIsolation"
})
@ConfigGroups(value = MySQLBinLogGroups.class)
@GenerateResourceBundle
public class MySQLBinLogDSource extends MysqlBinLogSource {

  @ConfigDefBean
  public MysqlBinLogSourceConfig config;

  public MysqlBinLogSourceConfig getConfig() {
    return config;
  }
}
