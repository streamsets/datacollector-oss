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
import com.streamsets.pipeline.api.ListBeanModel;

import java.util.List;

public class TensorFlowConfigBean {
  public static final String MODEL_PATH_CONFIG = "conf.modelPath";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Saved Model Path",
      description = "Local path to the model. Absolute path, or relative to the Data Collector resources directory.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "TENSOR_FLOW"
  )
  public String modelPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      defaultValue = "",
      label = "Model Tags",
      description = "Tags applied to the TensorFlow model",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "TENSOR_FLOW"
  )
  @ListBeanModel
  public List<String> modelTags;


  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Input Configs",
      description = "Input tensor information as configured during the training and exporting of the model",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "TENSOR_FLOW"
  )
  @ListBeanModel
  public List<TensorInputConfig> inputConfigs;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Output Configs",
      description = "Output tensor information as configured during the training and exporting of the model",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "TENSOR_FLOW"
  )
  @ListBeanModel
  public List<TensorConfig> outputConfigs;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Entire Batch",
      description = "Evaluates the full batch at once. Select when the TensorFlow model expects many inputs to " +
          "generate one output. Clear when the TensorFlow model expects one input to generate one output.",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "TENSOR_FLOW"
  )
  public boolean useEntireBatch = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "/output",
      label = "Output Field",
      description = "Output field for the prediction or classification result",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "TENSOR_FLOW",
      dependsOn = "useEntireBatch",
      triggeredByValue = "false"
  )
  @FieldSelectorModel(singleValued = true)
  public String outputField;
}
