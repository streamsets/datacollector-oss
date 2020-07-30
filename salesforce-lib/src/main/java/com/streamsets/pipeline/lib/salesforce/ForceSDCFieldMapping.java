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
package com.streamsets.pipeline.lib.salesforce;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

public class ForceSDCFieldMapping {

  /**
   * Constructor used for unit testing purposes
   * @param salesforceField
   * @param sdcField
   */
  public ForceSDCFieldMapping(final String salesforceField, final String sdcField) {
    this(salesforceField, sdcField, "", DataType.USE_SALESFORCE_TYPE);
  }

  /**
   * Constructor used for unit testing purposes
   * @param salesforceField
   * @param sdcField
   * @param defaultValue
   * @param dataType
   */
  public ForceSDCFieldMapping(
      final String salesforceField,
      final String sdcField,
      final String defaultValue,
      final DataType dataType
  ) {
    this.salesforceField = salesforceField;
    this.sdcField = sdcField;
    this.defaultValue = defaultValue;
    this.dataType = dataType;
  }

  /**
   * Parameter-less constructor required.
   */
  public ForceSDCFieldMapping() {}

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue="",
      label = "Salesforce Field",
      description = "The Salesforce field name.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String salesforceField;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "SDC Field",
      description = "The field in the record to receive the value.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @FieldSelectorModel(singleValued = true)
  public String sdcField;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Default Value",
      description = "The default value to be used when Salesforce returns no row. " +
          "If not set, the Missing Values Behavior applies.",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String defaultValue;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "USE_SALESFORCE_TYPE",
      label = "Data Type",
      description = "The field type. By default, the field type from Salesforce will be used. " +
          "But if the field type is provided, it will overwrite the Salesforce type. " +
          "Note that if the default value is provided, the field type must also be provided.",
      displayPosition = 40
  )
  @ValueChooserModel(DataTypeChooserValues.class)
  public DataType dataType;
}
