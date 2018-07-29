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
import com.streamsets.pipeline.api.FieldSelectorModel;

import java.util.List;

public class TensorInputConfig extends TensorConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "Fields to Convert",
      description = "You can convert multiple fields to the same type",
      displayPosition = 20
  )
  @FieldSelectorModel
  public List<String> fields;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      defaultValue= "",
      label = "Shape",
      description = "Shape of the tensor",
      displayPosition = 20
  )
  public List<Integer> shape;
}
