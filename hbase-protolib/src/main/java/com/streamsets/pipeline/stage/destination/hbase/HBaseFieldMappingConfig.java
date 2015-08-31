/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in
 * whole or part without written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hbase;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

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
      label = "Field Path",
      description = "The field path in the incoming record to output",
      displayPosition = 10)
  @FieldSelectorModel(singleValued = true)
  public String columnValue;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Column",
      description = "The column to write this field into. Use format <COLUMNFAMILY>:<QUALIFIER>. " +
        "The column family must exist",
      displayPosition = 20)
  public String columnName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TEXT",
      label = "Storage Type",
      description = "The storage type for column",
      displayPosition = 30)
  @ValueChooserModel(StorageTypeChooserValues.class)
  public StorageType columnStorageType;
}
