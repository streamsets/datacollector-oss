/**
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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.CDCSourceConfigBean;

public class OracleCDCConfigBean {

  @ConfigDefBean
  public CDCSourceConfigBean baseConfigBean = new CDCSourceConfigBean();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "PDB",
      description = "The pluggable database containing the database. Required for Oracle 12c if PDB is used.",
      displayPosition = 50,
      group = "JDBC"
  )
  public String pdb;

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
      displayPosition = 50,
      group = "CDC",
      dependsOn = "startValue",
      triggeredByValue = "DATE"
  )
  public String startDate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Start SCN",
      description = "System change number to use for the initial change",
      displayPosition = 60,
      group = "CDC",
      dependsOn = "startValue",
      triggeredByValue = "SCN"
  )
  public String startSCN;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Maximum Transaction Length",
      description = "Time window to look for changes within a transaction before commit (in seconds)",
      displayPosition = 70,
      group = "CDC",
      elDefs = TimeEL.class,
      defaultValue = "${1 * HOURS}"
  )
  public long txnWindow;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "LogMiner Session Window",
      description = "Time window of time a LogMiner session should be kept open. " +
          "Must be greater than or equal to Maximum Transaction Length. " +
          "Keeping this small will reduce memory usage on Oracle.",
      displayPosition = 70,
      group = "CDC",
      elDefs = TimeEL.class,
      defaultValue = "${2 * HOURS}"
  )
  public long logminerWindow;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Dictionary Source",
      description = "Location of the LogMiner dictionary",
      displayPosition = 80,
      group = "CDC"
  )
  @ValueChooserModel(DictionaryChooserValues.class)
  public DictionaryValues dictionary;

}
