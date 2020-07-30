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
package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

public class JdbcFieldColumnMapping {

  /**
   * Constructor used for unit testing purposes
   * @param columnName
   * @param field
   */
  public JdbcFieldColumnMapping(final String columnName, final String field) {
    this(columnName, field, "", DataType.USE_COLUMN_TYPE);
  }

  /**
   * Constructor used for unit testing purposes
   * @param columnName
   * @param field
   * @param defaultValue
   * @param dataType
   */
  public JdbcFieldColumnMapping(final String columnName, final String field, final String defaultValue, final DataType dataType) {
    this.columnName = columnName;
    this.field = field;
    this.defaultValue = defaultValue;
    this.dataType = dataType;
  }

  /**
   * Parameter-less constructor required.
   */
  public JdbcFieldColumnMapping() {}

  @ConfigDef(
          displayMode = ConfigDef.DisplayMode.BASIC,
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue="",
          label = "Column Name",
          description = "The database column name.",
          displayPosition = 10
  )
  public String columnName;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "SDC Field",
      description = "The field in the record to receive the value.",
      displayPosition = 20
  )
  @FieldSelectorModel(singleValued = true)
  public String field;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Default Value",
      description = "The default value to be used when the database returns no row. " +
          "If not set, the Missing Values Behavior applies.",
      displayPosition = 30
  )
  public String defaultValue;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "USE_COLUMN_TYPE",
      label = "Data Type",
      description = "The field type. By default, the column type from the database will be used. " +
          "But if the field type is provided, it will overwrite the column type. " +
          "Note that if the default value is provided, the field type must also be provided.",
      displayPosition = 40
  )
  @ValueChooserModel(DataTypeChooserValues.class)
  public DataType dataType;
}
