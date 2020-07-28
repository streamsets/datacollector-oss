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
package com.streamsets.pipeline.stage.destination.bigtable;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

public class BigtableFieldMapping {

  @VisibleForTesting
  BigtableFieldMapping(String column, String source, BigtableStorageType storageType) {
    this.column = column;
    this.source = source;
    this.storageType = storageType;
  }

  public BigtableFieldMapping() {
  }

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "/",
      label = "Field Path",
      description = "Field path in the incoming record",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @FieldSelectorModel(singleValued = true)
  public String source;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Column",
      description = "The column to write into. Enable Explicit Column Family Mapping and use format <COLUMNFAMILY>:<QUALIFIER> " +
          "to override the default column family",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String column;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TEXT",
      label = "Storage Type",
      description = "The storage type for column",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(BigtableStorageTypeChooserValues.class)
  public BigtableStorageType storageType;
}
