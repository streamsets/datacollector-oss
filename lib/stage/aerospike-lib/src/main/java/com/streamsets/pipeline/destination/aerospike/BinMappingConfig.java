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
package com.streamsets.pipeline.destination.aerospike;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;

public class BinMappingConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Bin",
      description = "Expression to get bin name",
      displayPosition = 20
  )
  public String binExpr;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Value",
      description = "Expression to get value",
      defaultValue="${record:attribute('/val')}",
      displayPosition = 30
  )
  public String valueExpr;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="STRING",
      label = "Partition Value Type",
      description="Partition column's value type",
      displayPosition = 20
  )
  @ValueChooserModel(DataTypeChooserValues.class)
  public DataType valueType = DataType.STRING;

  public BinMappingConfig() {
  }
}
