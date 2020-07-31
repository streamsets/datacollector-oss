/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.tensorflow;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import org.tensorflow.DataType;

public class TensorConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Operation",
      description = "Operation",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "TENSOR_FLOW"
  )
  public String operation;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "",
      label = "Index",
      description = "Index",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "TENSOR_FLOW"
  )
  public int index;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Tensor Type",
      description = "Datatype of the tensor",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "TENSOR_FLOW"
  )
  @ValueChooserModel(DataTypeChooserValues.class)
  public DataType tensorDataType;
}
