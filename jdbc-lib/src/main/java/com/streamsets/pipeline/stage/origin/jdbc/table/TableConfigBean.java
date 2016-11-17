/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TableConfigBean {
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Schema Name",
      description = "Schema Name",
      displayPosition = 20,
      group = "JDBC"
  )
  public String schema;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Table Name Pattern",
      description = "Table Name Pattern. Use a SQL Like Syntax",
      displayPosition = 30,
      defaultValue = "%",
      group = "JDBC"
  )
  public String tablePattern;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Table Exclusion Pattern",
      description = "Table Exclusion Pattern. Use a Java Regex Syntax. Leave empty if no exclusion needed.",
      displayPosition = 40,
      group = "JDBC"
  )
  public String tableExclusionPattern;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Override Partition Columns",
      description = "Overrides the primary key as the partition column.",
      displayPosition = 50,
      defaultValue = "false",
      group = "JDBC"
  )
  public boolean overridePartitionColumns;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Partition Columns",
      displayPosition  = 60,
      group = "JDBC",
      dependsOn = "overridePartitionColumns",
      triggeredByValue = "true"
  )
  @ListBeanModel
  public List<String> partitionColumns = new ArrayList<>();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Initial Offset",
      description = "Configure Initial Offset for each Offset Column.",
      displayPosition = 70,
      group = "JDBC"
  )
  public Map<String, String> offsetColumnToInitialOffsetValue = new LinkedHashMap<>();

}
