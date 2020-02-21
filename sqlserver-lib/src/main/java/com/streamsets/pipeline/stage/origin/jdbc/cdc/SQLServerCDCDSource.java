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
package com.streamsets.pipeline.stage.origin.jdbc.cdc;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;
import com.streamsets.pipeline.stage.config.SQLServerHikariPoolConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.sqlserver.CDCTableJdbcConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.sqlserver.SQLServerCDCSource;
import com.streamsets.pipeline.stage.origin.jdbc.table.QuoteChar;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcConfigBean;

/*@StageDef(version = 1,
    label = "SQL Server CDC Client",
    description = "Reads data from Microsoft SQL Server change data capture (CDC) tables",
    icon = "sql-server-multithreaded.png",
    resetOffset = true,
    producesEvents = true,
    upgraderDef = "upgrader/SQLServerCDCDSource.yaml",
    onlineHelpRefUrl = "index.html?contextID=task_nsg_fxc_v1b",
    services = @ServiceDependency(service = SshTunnelService.class))*/
@GenerateResourceBundle
@ConfigGroups(value = Groups.class)
public class SQLServerCDCDSource extends DPushSource {

  @ConfigDefBean
  public SQLServerHikariPoolConfigBean hikariConfigBean;

  @ConfigDefBean
  public CommonSourceConfigBean commonSourceConfigBean = new CommonSourceConfigBean();

  @ConfigDefBean
  public CDCTableJdbcConfigBean cdcTableJdbcConfigBean = new CDCTableJdbcConfigBean();

  @Override
  protected PushSource createPushSource() {
    return new SQLServerCDCSource(
        hikariConfigBean,
        commonSourceConfigBean,
        cdcTableJdbcConfigBean,
        convertToTableJdbcConfigBean(cdcTableJdbcConfigBean)
    );
  }

  private TableJdbcConfigBean convertToTableJdbcConfigBean(CDCTableJdbcConfigBean cdcTableJdbcConfigBean) {
    TableJdbcConfigBean tableJdbcConfigBean = new TableJdbcConfigBean();
    tableJdbcConfigBean.batchTableStrategy = cdcTableJdbcConfigBean.batchTableStrategy;
    tableJdbcConfigBean.quoteChar = QuoteChar.NONE;
    tableJdbcConfigBean.timeZoneID = "UTC";
    tableJdbcConfigBean.numberOfThreads = cdcTableJdbcConfigBean.numberOfThreads;
    tableJdbcConfigBean.tableOrderStrategy = cdcTableJdbcConfigBean.tableOrderStrategy;
    tableJdbcConfigBean.unknownTypeAction = cdcTableJdbcConfigBean.unknownTypeAction;

    return tableJdbcConfigBean;
  }
}
