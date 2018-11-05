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
package com.streamsets.pipeline.stage.processor.jdbcmetadata;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;

public class JdbcMetadataConfigBean {
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      label = "Schema Name",
      displayPosition = 20,
      group = "JDBC"
  )
  public String schemaEL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      defaultValue = "${record:attribute('tableName')}",
      label = "Table Name",
      description = "Table Names should contain only table names. Schema should be defined in the connection string or " +
          "schema configuration",
      displayPosition = 30,
      group = "JDBC"
  )
  public String tableNameEL;

  @ConfigDefBean
  public HikariPoolConfigBean hikariConfigBean;

  @ConfigDefBean
  public DecimalDefaultsConfig decimalDefaultsConfig;
}
