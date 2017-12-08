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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.sqlserver;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DPushSource;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.QuoteChar;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcConfigBean;

@StageDef(
    version = 4,
    label = "SQL Server CDC Client",
    description = "Origin that an read change events from an MS SQL Server Database",
    icon = "sql-server-multithreaded.png",
    resetOffset = true,
    producesEvents = true,
    upgrader = SQLServerCDCSourceUpgrader.class,
    onlineHelpRefUrl = "index.html#Origins/SQLServerCDC.html#task_nsg_fxc_v1b"
)
@GenerateResourceBundle
@ConfigGroups(Groups.class)
public class SQLServerCDCDSource extends DPushSource {

  @ConfigDefBean
  public HikariPoolConfigBean hikariConf = new HikariPoolConfigBean();

  @ConfigDefBean
  public CommonSourceConfigBean commonSourceConfigBean = new CommonSourceConfigBean();

  @ConfigDefBean
  public CDCTableJdbcConfigBean cdcTableJdbcConfigBean = new CDCTableJdbcConfigBean();

  @Override
  protected PushSource createPushSource() {
    return new SQLServerCDCSource(hikariConf, commonSourceConfigBean, cdcTableJdbcConfigBean, convertToTableJdbcConfigBean(cdcTableJdbcConfigBean));
  }

  private TableJdbcConfigBean convertToTableJdbcConfigBean(CDCTableJdbcConfigBean cdcTableJdbcConfigBean) {
    TableJdbcConfigBean tableJdbcConfigBean = new TableJdbcConfigBean();
    tableJdbcConfigBean.batchTableStrategy = cdcTableJdbcConfigBean.batchTableStrategy;
    tableJdbcConfigBean.quoteChar = QuoteChar.NONE;
    tableJdbcConfigBean.timeZoneID = "UTC";
    tableJdbcConfigBean.numberOfThreads = cdcTableJdbcConfigBean.numberOfThreads;
    tableJdbcConfigBean.tableOrderStrategy = cdcTableJdbcConfigBean.tableOrderStrategy;

    return tableJdbcConfigBean;
  }

}
