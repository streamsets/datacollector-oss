/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in
 * whole or part without written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hbase;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.ValueChooser;

public class HBaseFieldMappingConfig {

  @VisibleForTesting
  HBaseFieldMappingConfig(String columnName, String columnValue, StorageType columnStorageType) {
    this.columnName = columnName;
    this.columnValue = columnValue;
    this.columnStorageType = columnStorageType;
  }

  /**
   * Parameter-less constructor required.
   */
  public HBaseFieldMappingConfig() {
  }

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "/",
      label = "Field-path",
      description = "The field-path in the incoming record to output",
      displayPosition = 10)
  @FieldSelector(singleValued = true)
  public String columnValue;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Column",
      description = "The column (columnfamily:qualifier) to write this field into. Make sure "
          + "the column family exist",
      displayPosition = 20)
  public String columnName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TEXT",
      label = "Storage type",
      description = "The storage type for column. It can be either text, binary or json string",
      displayPosition = 30)
  @ValueChooser(StorageTypeChooserValues.class)
  public StorageType columnStorageType;
}
