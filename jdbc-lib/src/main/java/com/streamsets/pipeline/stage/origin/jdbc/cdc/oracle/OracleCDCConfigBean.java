/**
 * Copyright 2015 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import com.streamsets.pipeline.stage.origin.jdbc.cdc.CDCSourceConfigBean;

public class OracleCDCConfigBean {

  @ConfigDefBean
  public CDCSourceConfigBean baseConfigBean = new CDCSourceConfigBean();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "PDB",
      description = "The pluggable database containing the database. Required for Oracle 12+",
      displayPosition = 50,
      group = "JDBC"
  )
  public String pdb;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Initial Change",
      description = "Criteria to download Initial change",
      displayPosition = 35,
      group = "CDC",
      defaultValue = "LATEST"
  )
  @ValueChooserModel(StartChooserValues.class)
  public StartValues startValue;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Start Date (DD-MON-YYYY HH24:MM:SS)",
      description = "The Date from which the first change must be downloaded. Date must be in this format: " +
          "DD-MON-YYYY HH24:MM:SS. Example: 01-Jan-2016 22:15:35. Archived Redo logs must be available from that date",
      displayPosition = 40,
      group = "CDC",
      dependsOn = "startValue",
      triggeredByValue = "DATE"
  )
  public String startDate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Start SCN/Change number",
      description = "The SCN of the oldest change to download",
      displayPosition = 45,
      group = "CDC",
      dependsOn = "startValue",
      triggeredByValue = "SCN"
  )
  public String startSCN;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Dictionary Source",
      description = "Where to find the dictionary from. " +
          "If online dictionary is used, schema changes will render older difficult to read. " +
          "If redo logs are used, you must add the new schema to the redo logs when schema is changed",
      displayPosition = 50,
      group = "CDC"
  )
  @ValueChooserModel(DictionaryChooserValues.class)
  public DictionaryValues dictionary;

}
