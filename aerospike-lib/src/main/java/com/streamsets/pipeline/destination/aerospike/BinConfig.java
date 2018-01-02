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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

public class BinConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Bin name expression",
      description = "Expression language to obtain bin name from record",
      defaultValue = "${record:value('/bin_name_1')}",
      displayPosition = 10,
      elDefs = {RecordEL.class, StringEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String binName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Bin value expression",
      description = "Expression language to obtain bin value from record",
      defaultValue = "${record:value('/bin_val_1')}",
      displayPosition = 20,
      elDefs = {RecordEL.class, StringEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT

  )
  public String binValue;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "STRING",
      label = "Partition Value Type",
      description = "Partition column's value type",
      displayPosition = 30
  )
  @ValueChooserModel(DataTypeChooserValues.class)
  public DataType valueType = DataType.STRING;

  public BinConfig() {
  }

  @VisibleForTesting
  BinConfig(String binName, String binValue, DataType valueType) {
    this.binName = binName;
    this.binValue = binValue;
    this.valueType = valueType;
  }
}
