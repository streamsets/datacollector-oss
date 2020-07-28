/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.scripting.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.stage.util.scripting.config.ScriptRecordType;
import com.streamsets.pipeline.stage.util.scripting.config.ScriptRecordTypeValueChooser;

import java.util.Map;

public class ScriptSourceConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Batch Size",
      description = "Number of records to generate in a single batch.\n" +
          "Access in user script with sdc.batchSize.",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 1,
      max = Integer.MAX_VALUE,
      group = "PERFORMANCE"
  )
  public int batchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1",
      label = "Number of Threads",
      description = "Number of concurrent threads that generate data in parallel.\n" +
          "Access in user script with sdc.numThreads.",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 1,
      max = Integer.MAX_VALUE,
      group = "PERFORMANCE"
  )
  public int numThreads;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NATIVE_OBJECTS",
      label = "Record Type",
      description = "Record type to use during script execution",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  @ValueChooserModel(ScriptRecordTypeValueChooser.class)
  public ScriptRecordType scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;

  @ConfigDef(
      required = false,
      defaultValue = "{}",
      type = ConfigDef.Type.MAP,
      label = "Parameters in Script",
      description = "Parameters and values for use in script.\n" +
          "Access in user script as sdc.userParams.",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  public Map<String, String> params;

}
