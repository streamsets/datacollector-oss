/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.parser.sql;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;

public class FieldTypeBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Name",
      displayPosition = 10,
      group = "PARSE"
  )
  public String name;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "JDBC Type",
      displayPosition = 20,
      group = "JDBC"
  )
  @ValueChooserModel(JDBCTypeChooserValues.class)
  public JDBCTypes type;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Precision",
      displayPosition = 20,
      dependsOn = "type",
      triggeredByValue = {"NUMERIC", "DECIMAL"},
      group = "JDBC"
  )
  public int precision;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Scale",
      displayPosition = 20,
      dependsOn = "type",
      triggeredByValue = {"NUMERIC", "DECIMAL"},
      group = "JDBC"
  )
  public int scale;

}
