/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.jdbc;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DSource;
import com.streamsets.pipeline.lib.el.TimeEL;

import java.util.Map;

@StageDef(
    version = "1.0.0",
    label = "JDBC Consumer",
    description = "Reads data from a JDBC source.",
    icon = "rdbms.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class JdbcDSource extends DSource {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "JDBC Connection String",
      displayPosition = 10,
      group = "JDBC"
  )
  public String connectionString;

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
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Use Credentials",
      displayPosition = 100,
      group = "JDBC"
  )
  public boolean useCredentials;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      dependsOn = "useCredentials",
      triggeredByValue = "true",
      label = "Username",
      displayPosition = 110,
      group = "CREDENTIALS"
  )
  public String username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      dependsOn = "useCredentials",
      triggeredByValue = "true",
      label = "Password",
      displayPosition = 120,
      group = "CREDENTIALS"
  )
  public String password;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "",
      label = "Additional JDBC Configuration Properties",
      description = "Additional properties to pass to the underlying JDBC driver.",
      displayPosition = 130,
      group = "JDBC"
  )
  public Map<String, String> driverProperties;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "JDBC Driver Class Name",
      description = "Class name for pre-JDBC 4 compliant drivers.",
      displayPosition = 140,
      group = "LEGACY"
  )
  public String driverClassName;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.SQL,
      label = "Connection Health Test Query",
      description = "Not recommended for JDBC 4 compliant drivers. Runs when a new database connection is established.",
      displayPosition = 150,
      group = "LEGACY"
  )
  public String connectionTestQuery;

  @Override
  protected Source createSource() {
    return new JdbcSource(
        isIncrementalMode,
        connectionString,
        query,
        initialOffset,
        offsetColumn,
        queryInterval,
        username,
        password,
        driverProperties,
        driverClassName,
        connectionTestQuery
      );
  }
}
