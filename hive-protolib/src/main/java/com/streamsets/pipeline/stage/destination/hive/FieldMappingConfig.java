/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hive;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;

public class FieldMappingConfig {

  /**
   * Constructor used for unit testing purposes
   * @param field
   * @param columnName
   */
  public FieldMappingConfig(final String field, final String columnName) {
    this.field = field;
    this.columnName = columnName;
  }

  /**
   * Parameter-less constructor required.
   */
  public FieldMappingConfig() {}

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "SDC Field",
      description = "The field in the incoming record to output.",
      displayPosition = 10
  )
  @FieldSelectorModel(singleValued = true)
  public String field;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue="",
      label = "Column Name",
      description="The column name to write this field to.",
      displayPosition = 20
  )
  public String columnName;
}
