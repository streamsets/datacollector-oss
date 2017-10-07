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
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DPushSource;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;

@StageDef(
    version = 4,
    label = "JDBC Multitable Consumer",
    description = "Reads data from a JDBC source using table names.",
    icon = "rdbms_multithreaded.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    resetOffset = true,
    producesEvents = true,
    upgrader = TableJdbcSourceUpgrader.class,
    onlineHelpRefUrl = "index.html#Origins/MultiTableJDBCConsumer.html#task_kst_m4w_4y"
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public final class TableJdbcDSource extends DPushSource {

  @ConfigDefBean
  public TableJdbcConfigBean tableJdbcConfigBean;

  @ConfigDefBean
  public CommonSourceConfigBean commonSourceConfigBean;

  @ConfigDefBean
  public HikariPoolConfigBean hikariConfigBean;

  @Override
  protected PushSource createPushSource() {
    return new TableJdbcSource(
        hikariConfigBean,
        commonSourceConfigBean,
        tableJdbcConfigBean
    );
  }
}
