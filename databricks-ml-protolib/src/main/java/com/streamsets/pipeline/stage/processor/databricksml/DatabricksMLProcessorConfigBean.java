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
package com.streamsets.pipeline.stage.processor.databricksml;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;

import java.util.ArrayList;
import java.util.List;

@GenerateResourceBundle
public class DatabricksMLProcessorConfigBean {
  public static final String MODEL_PATH_CONFIG = "conf.modelPath";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Saved Model Path",
      description = "Local path to the Spark-trained model. " +
          "Absolute path, or relative to the Data Collector resources directory.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DATABRICKS_ML"
  )
  public String modelPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Model Output Columns",
      description = "Select the output columns that the model should return",
      defaultValue = "[\"label\", \"prediction\", \"probability\"]",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DATABRICKS_ML"
  )
  public List<String> outputColumns = new ArrayList<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Input Root Field",
      description = "JSON value of the input root field to be passed as model input",
      defaultValue = "/",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DATABRICKS_ML"
  )
  @FieldSelectorModel(singleValued = true)
  public String inputField = "/";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "/output",
      label = "Output Field",
      description = "Field to store the model output",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DATABRICKS_ML"
  )
  @FieldSelectorModel(singleValued = true)
  public String outputField = "/output";

}
