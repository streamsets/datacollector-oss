/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.jdbc;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.configurablestage.DSource;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;

@StageDef(
    version = 5,
    label = "JDBC Consumer",
    description = "Reads data from a JDBC source.",
    icon = "rdbms.png",
    execution = ExecutionMode.STANDALONE,
    upgrader = JdbcSourceUpgrader.class,
    recordsByRef = true
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class JdbcDSource extends DSource {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Incremental Mode",
      description = "Disabling Incremental Mode will always substitute the value in" +
          " Initial Offset in place of ${OFFSET} instead of the most recent value of <offsetColumn>.",
      displayPosition = 15,
      group = "JDBC"
  )
  public boolean isIncrementalMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.SQL,
      label = "SQL Query",
      description = "SELECT <offset column>, ... FROM <table name> WHERE <offset column>  >  ${OFFSET} ORDER BY <offset column>",
      elDefs = {OffsetEL.class},
      evaluation = ConfigDef.Evaluation.IMPLICIT,
      displayPosition = 20,
      group = "JDBC"
  )
  public String query;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Initial Offset",
      description = "Initial value to insert for ${offset}." +
          " Subsequent queries will use the result of the Next Offset Query",
      displayPosition = 40,
      group = "JDBC"
  )
  public String initialOffset;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Offset Column",
      description = "Column checked to track current offset.",
      displayPosition = 50,
      group = "JDBC"
  )
  public String offsetColumn;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "${10 * SECONDS}",
      label = "Query Interval",
      displayPosition = 60,
      elDefs = {TimeEL.class},
      evaluation = ConfigDef.Evaluation.IMPLICIT,
      group = "JDBC"
  )
  public long queryInterval;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "LIST_MAP",
      label = "Record Type",
      displayPosition = 130,
      group = "JDBC"
  )
  @ValueChooserModel(JdbcRecordTypeChooserValues.class)
  public JdbcRecordType jdbcRecordType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Size (Records)",
      displayPosition = 140,
      group = "JDBC"
  )
  public int maxBatchSize;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Transaction ID Column Name",
      description = "When reading a change data table, column identifying the transaction the change belongs to.",
      displayPosition = 180,
      group = "CDC"
  )
  public String txnIdColumnName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Transaction Size",
      description = "If transactions exceed this size, they will be applied in multiple batches.",
      defaultValue = "10000",
      displayPosition = 190,
      group = "CDC"
  )
  public int txnMaxSize;

  @ConfigDefBean()
  public HikariPoolConfigBean hikariConfigBean;

  @Override
  protected Source createSource() {
    return new JdbcSource(
        isIncrementalMode,
        query,
        initialOffset,
        offsetColumn,
        queryInterval,
        txnIdColumnName,
        txnMaxSize,
        jdbcRecordType,
        maxBatchSize,
        hikariConfigBean
      );
  }
}
