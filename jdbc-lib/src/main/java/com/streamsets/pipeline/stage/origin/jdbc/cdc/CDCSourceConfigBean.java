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

import java.util.List;

public class CDCSourceConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Maximum Batch Size",
      description = "Maximum number of change events per batch",
      displayPosition = 30,
      defaultValue = "100",
      group = "JDBC"
  )
  public int maxBatchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Database",
      description = "The database to track changes to",
      displayPosition = 40,
      group = "CDC"
  )
  public String database;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Tables",
      description = "List of tables to track changes to",
      displayPosition = 50,
      group = "CDC"
  )
  public List<String> tables;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Case Sensitive DB/Table names",
      description = "If unchecked, upper case will be used. " +
          "Check this only if the schema and table names were quoted during creation (not common)",
      displayPosition = 50,
      group = "CDC",
      defaultValue = "false"
  )
  public boolean caseSensitive;
}
