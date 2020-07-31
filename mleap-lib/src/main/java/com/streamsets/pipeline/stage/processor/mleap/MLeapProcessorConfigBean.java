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
package com.streamsets.pipeline.stage.processor.mleap;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ListBeanModel;

import java.util.ArrayList;
import java.util.List;

@GenerateResourceBundle
public class MLeapProcessorConfigBean {
  public static final String MODEL_PATH_CONFIG = "conf.modelPath";
  public static final String INPUT_CONFIGS_CONFIG = "conf.inputConfigs";
  public static final String OUTPUT_FIELD_NAMES_CONFIG = "conf.outputFieldNames";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Saved Model File Path",
      description = "Local path to the MLeap model archive file or directory. " +
          "Absolute path, or relative to the Data Collector resources directory.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MLEAP"
  )
  public String modelPath;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Input Configs",
      description = "Mapping of MLeap input fields to fields in the record",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MLEAP"
  )
  @ListBeanModel
  public List<InputConfig> inputConfigs = new ArrayList<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Model Output Fields",
      description = "Select the output fields that the model should return",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MLEAP"
  )
  public List<String> outputFieldNames = new ArrayList<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "/output",
      label = "Output Field",
      description = "Field to store the model output",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MLEAP"
  )
  @FieldSelectorModel(singleValued = true)
  public String outputField = "/output";

}
