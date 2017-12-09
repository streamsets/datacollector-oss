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
package com.streamsets.pipeline.stage.origin.jdbc.cdc;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;

import java.util.List;

public class SchemaTableConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Schema",
      description = "Schema Name",
      displayPosition = 10,
      group = "#0"
  )
  public String schema;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Table Name Patterns",
      description = "Patterns of the table names to read. Use a SQL like syntax.",
      displayPosition = 20,
      group = "#0"
  )
  @ListBeanModel
  public List<String> tables;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Table Exclusion Pattern",
      description = "Pattern of the table names to exclude from being read. Use a Java regex syntax." +
          " Leave empty if no exclusion needed.",
      displayPosition = 30,
      group = "#0"
  )
  public String excludePattern;

}
