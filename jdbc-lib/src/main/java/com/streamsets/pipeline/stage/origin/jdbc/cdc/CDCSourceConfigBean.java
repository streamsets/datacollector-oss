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
package com.streamsets.pipeline.stage.origin.jdbc.cdc;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.MultiValueChooserModel;

import java.util.List;

public class CDCSourceConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Batch Size (records)",
      description = "Maximum number of records in a batch",
      displayPosition = 30,
      defaultValue = "100",
      group = "JDBC"
  )
  public int maxBatchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Schema Name",
      displayPosition = 10,
      group = "CDC"
  )
  public String database;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Tables",
      displayPosition = 20,
      group = "CDC"
  )
  public List<String> tables;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "[\"INSERT\", \"UPDATE\", \"DELETE\", \"SELECT_FOR_UPDATE\"]",
      label = "Operations",
      description = "Operations to capture as records. All other operations are ignored.",
      displayPosition = 70,
      group = "CDC"
  )
  @MultiValueChooserModel(ChangeTypesChooserValues.class)
  public List<ChangeTypeValues> changeTypes;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Case Sensitive Names",
      description = "Use for lower or mixed-case database, table and field names. " +
          "By default, names are changed to all caps. " +
          "Select only when the database or tables were created with quotation marks around the names.",
      displayPosition = 30,
      group = "CDC",
      defaultValue = "false"
  )
  public boolean caseSensitive;
}
