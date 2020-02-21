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

package com.streamsets.pipeline.stage.processor.jdbctee;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.lib.jdbc.EncryptionGroups;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.stage.config.SQLServerHikariPoolConfigBean;

//@StageDef(version = 4,
//    label = "SQLServer Tee",
//    description = "Write records to SQLServer and enrich records with generated columns",
//    upgrader = JdbcTeeUpgrader.class,
//    icon = "sql-server.png",
//    onlineHelpRefUrl = "index.html?contextID=task_yrw_l3v_33b")
@ConfigGroups(value = EncryptionGroups.class)
@GenerateResourceBundle
@HideConfigs(value = {
    "hikariConfigBean.encrypt",
    "hikariConfigBean.readOnly",
    "hikariConfigBean.autoCommit",
})
public class SQLServerTeeDProcessor extends JdbcTeeDProcessor {

  @ConfigDefBean
  public SQLServerHikariPoolConfigBean hikariConfigBean;

  @Override
  protected HikariPoolConfigBean getHikariConfigBean() {
    return hikariConfigBean;
  }

  @Override
  protected Processor createProcessor() {
    return new JdbcTeeProcessor(
        schema,
        tableNameTemplate,
        customMappings,
        generatedColumnMappings, encloseTableName,
        rollbackOnError,
        useMultiRowOp,
        maxPrepStmtParameters,
        changeLogFormat,
        getHikariConfigBean(),
        defaultOperation,
        unsupportedAction
    );
  }

}
