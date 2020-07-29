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
package com.streamsets.pipeline.stage.destination.hbase;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.hbase.api.common.producer.StorageType;
import com.streamsets.pipeline.hbase.api.common.producer.StorageTypeChooserValues;

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
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @FieldSelectorModel(singleValued = true)
  public String columnValue;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Column",
      description = "The column to write this field into. Use format <COLUMNFAMILY>:<QUALIFIER>. " +
        "The column family must exist",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String columnName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TEXT",
      label = "Storage Type",
      description = "The storage type for column",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(StorageTypeChooserValues.class)
  public StorageType columnStorageType;
}
