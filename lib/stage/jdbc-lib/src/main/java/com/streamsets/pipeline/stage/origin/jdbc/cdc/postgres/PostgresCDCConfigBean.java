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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.MultiValueChooserModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.jdbc.parser.sql.UnsupportedFieldTypeChooserValues;
import com.streamsets.pipeline.lib.jdbc.parser.sql.UnsupportedFieldTypeValues;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.CDCSourceConfigBean;
import java.util.List;

public class PostgresCDCConfigBean {

  @ConfigDefBean
  public CDCSourceConfigBean baseConfigBean = new CDCSourceConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Initial Change",
      description = "Determines where to start reading",
      displayPosition = 40,
      group = "CDC",
      defaultValue = "LATEST"
  )
  @ValueChooserModel(StartChooserValues.class)
  public StartValues startValue;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Start Date",
      description = "Datetime to use for the initial change. Use the following format: DD-MM-YYYY HH24:MM:SS.",
      displayPosition = 45,
      group = "CDC",
      dependsOn = "startValue",
      triggeredByValue = "DATE"
  )
  public String startDate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "DB Time Zone",
      description = "Time Zone that the DB is operating in",
      displayPosition = 45,
      group = "CDC",
      dependsOn = "startValue",
      triggeredByValue = "DATE"
  )
  @ValueChooserModel(TimeZoneChooserValues.class)
  public String dbTimeZone;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Start LSN",
      description = "Logical Sequence Number to use for the initial change",
      displayPosition = 45,
      group = "CDC",
      dependsOn = "startValue",
      triggeredByValue = "LSN"
  )
  public String startLSN;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      label = "Remove Replication Slot on Close",
      description = "Removing on close means no WAL updates for that slot will be generated, but "
          + "system performance will not be impacted.",
      displayPosition = 50,
      group = "CDC",
      defaultValue = "false")
  public boolean removeSlotOnClose;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Replication Slot",
      description = "Name of slot to create.",
      defaultValue="SDC",
      displayPosition = 50,
      group = "CDC"
  )
  public String slot;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Minimum PostgreSQL Version",
      description = "Minimum PostgreSQL version to assume.",
      defaultValue ="NINEFOUR",
      displayPosition = 50,
      group = "CDC"
  )
  @ValueChooserModel(PgVerChooserValues.class)
  public PgVersionValues minVersion;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "[\"INSERT\", \"UPDATE\", \"DELETE\"]",
      label = "Operations",
      description = "Operations to capture as records. All other operations are ignored.",
      displayPosition = 70,
      group = "CDC"
  )
  @MultiValueChooserModel(PostgresChangeTypesChooserValues.class)
  public List<PostgresChangeTypeValues> postgresChangeTypes;

  //HIDDEN - only choice supported today
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Replication Type",
      description = "Database support",
      defaultValue ="database",
      displayPosition = 50,
      group = "CDC"
  )
  public String replicationType;

  //HIDDEN - only choice supported today
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Output Decoder",
      description = "Output Decoder installed with Postgres",
      displayPosition = 50,
      group = "CDC",
      defaultValue = "WAL2JSON"
  )
  @ValueChooserModel(DecoderChooserValues.class)
  public DecoderValues decoderValue;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Unsupported Field Type",
      description = "Action to take if an unsupported field type is encountered.",
      displayPosition = 110,
      group = "CDC",
      defaultValue = "TO_ERROR"
  )
  @ValueChooserModel(UnsupportedFieldTypeChooserValues.class)
  public UnsupportedFieldTypeValues unsupportedFieldOp;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Add Unsupported Fields",
      description = "Add values of unsupported fields as unparsed strings to records",
      displayPosition = 115,
      group = "CDC",
      defaultValue = "false"
  )
  public boolean sendUnsupportedFields;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Query Timeout",
      description = "Time to wait before timing out a WAL query and returning the batch.",
      displayPosition = 140,
      group = "CDC",
      elDefs = TimeEL.class,
      defaultValue = "${5 * MINUTES}"
  )
  public int queryTimeout;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Poll Interval",
      description = "Interval between checking for CDC updates.",
      displayPosition = 140,
      group = "CDC",
      elDefs = TimeEL.class,
      defaultValue = "${60 * SECONDS}"
  )
  public int pollInterval;

  //HIDDEN
  @ConfigDef(
        required = false,
        type = ConfigDef.Type.BOOLEAN,
        label = "Parse SQL Query",
        description = "",
        displayPosition = 150,
        group = "CDC",
        defaultValue = "false"
  )
  public boolean parseQuery;
}
