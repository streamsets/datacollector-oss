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
package com.streamsets.pipeline.stage.origin.jdbc.CT;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;
import com.streamsets.pipeline.lib.jdbc.EncryptionGroups;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.CT.sqlserver.CTTableJdbcConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.CT.sqlserver.SQLServerCTSource;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.QuoteChar;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcConfigBean;

//@StageDef(version = 1,
//    label = "SQL Server Change Tracking Client",
//    description = "Origin that an read change events from an SQL Server Database",
//    icon = "sql-server-multithreaded.png",
//    resetOffset = true,
//    producesEvents = true,
//    upgraderDef = "upgrader/SQLServerCTDSource.yaml",
//    onlineHelpRefUrl = "index.html?contextID=task_vsh_22s_r1b")
@GenerateResourceBundle
@ConfigGroups(value = EncryptionGroups.class)
@HideConfigs({
    "commonSourceConfigBean.allowLateTable",
    "commonSourceConfigBean.enableSchemaChanges",
    "commonSourceConfigBean.txnWindow",
})
public class SQLServerCTDSource extends DPushSource {

  @ConfigDefBean
  public HikariPoolConfigBean hikariConf = new HikariPoolConfigBean();

  @ConfigDefBean
  public CommonSourceConfigBean commonSourceConfigBean = new CommonSourceConfigBean();

  @ConfigDefBean
  public CTTableJdbcConfigBean ctTableJdbcConfigBean = new CTTableJdbcConfigBean();

  @Override
  protected PushSource createPushSource() {
    return new SQLServerCTSource(
        hikariConf,
        commonSourceConfigBean,
        ctTableJdbcConfigBean,
        convertToTableJdbcConfigBean(ctTableJdbcConfigBean)
    );
  }

  private TableJdbcConfigBean convertToTableJdbcConfigBean(CTTableJdbcConfigBean ctTableJdbcConfigBean) {
    TableJdbcConfigBean tableJdbcConfigBean = new TableJdbcConfigBean();
    tableJdbcConfigBean.batchTableStrategy = ctTableJdbcConfigBean.batchTableStrategy;
    tableJdbcConfigBean.timeZoneID = ctTableJdbcConfigBean.timeZoneID;
    tableJdbcConfigBean.unknownTypeAction = ctTableJdbcConfigBean.unknownTypeAction;
    tableJdbcConfigBean.quoteChar = QuoteChar.NONE;
    tableJdbcConfigBean.numberOfThreads = ctTableJdbcConfigBean.numberOfThreads;
    tableJdbcConfigBean.tableOrderStrategy = ctTableJdbcConfigBean.tableOrderStrategy;

    return tableJdbcConfigBean;
  }

}
