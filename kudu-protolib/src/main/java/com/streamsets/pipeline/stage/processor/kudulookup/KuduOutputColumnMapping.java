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
package com.streamsets.pipeline.stage.processor.kudulookup;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;

public class KuduOutputColumnMapping {

  public KuduOutputColumnMapping() {}

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue="",
      label = "Column Name",
      description = "Column name in Kudu table",
      displayPosition = 10
  )
  public String columnName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "SDC Field",
      description = "Field to write in outgoing record",
      displayPosition = 20
  )
  @FieldSelectorModel(singleValued = true)
  public String field;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue="",
      label = "Default Value",
      description = "Default value to use if no value found in Kudu",
      displayPosition = 30
  )
  public String defaultValue;

  public KuduOutputColumnMapping(final String columnName, final String fieldName, final String defaultValue) {
    this.columnName = columnName;
    this.field = fieldName;
    this.defaultValue = defaultValue;
  }
}
