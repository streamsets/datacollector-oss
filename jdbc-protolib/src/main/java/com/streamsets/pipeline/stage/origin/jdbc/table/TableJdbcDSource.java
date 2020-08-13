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
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;
import com.streamsets.pipeline.lib.event.NoMoreDataEvent;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcHikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.multithread.SchemaFinishedEvent;
import com.streamsets.pipeline.lib.jdbc.multithread.TableFinishedEvent;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;

@StageDef(
    version = 10,
    label = "JDBC Multitable Consumer",
    description = "Reads data from a JDBC source using table names.",
    icon = "rdbms_multithreaded.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    resetOffset = true,
    producesEvents = true,
    eventDefs = {NoMoreDataEvent.class, SchemaFinishedEvent.class, TableFinishedEvent.class},
    upgrader = TableJdbcSourceUpgrader.class,
    upgraderDef = "upgrader/TableJdbcDSource.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_kst_m4w_4y"
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
@HideConfigs({
    "commonSourceConfigBean.allowLateTable",
    "commonSourceConfigBean.enableSchemaChanges",
    "commonSourceConfigBean.txnWindow"
})
public class TableJdbcDSource extends DPushSource {

  @ConfigDefBean
  public TableJdbcConfigBean tableJdbcConfigBean;

  @ConfigDefBean
  public CommonSourceConfigBean commonSourceConfigBean;

  @ConfigDefBean
  public JdbcHikariPoolConfigBean hikariConfigBean;

  /**
   * Returns the Hikari config bean.
   * <p/>
   * This method is used to pass the Hikari config bean to the underlaying connector.
   * <p/>
   * Subclasses may override this method to provide specific vendor configurations.
   * <p/>
   * IMPORTANT: when a subclass is overriding this method to return a specialized HikariConfigBean, the config property
   * itself in the connector subclass must have the same name as the config property in this class, this is
   * "hikariConfigBean".
   */
  protected HikariPoolConfigBean getHikariConfigBean() {
    return hikariConfigBean;
  }

  /**
   * Returns the TableJDBC config bean.
   * <p/>
   * This method is used to pass the TableJDBC config bean to the underlaying connector.
   * <p/>
   * Subclasses may override this method to provide specific vendor configurations.
   * <p/>
   * IMPORTANT: when a subclass is overriding this method to return a specialized TableJDBC, the config property
   * itself in the connector subclass must have the same name as the config property in this class.
   */
  protected TableJdbcConfigBean getTableJdbcConfigBean() {
    return tableJdbcConfigBean;
  }

  @Override
  protected PushSource createPushSource() {
    return new TableJdbcSource(
        getHikariConfigBean(),
        commonSourceConfigBean,
        getTableJdbcConfigBean()
    );
  }
}
