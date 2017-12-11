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
package com.streamsets.pipeline.stage.executor.s3.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

import java.util.Collections;
import java.util.Map;

public class S3TaskConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "CHANGE_EXISTING_OBJECT",
    label = "Task",
    description = "Task that should be performed.",
    group = "#0",
    displayPosition = 10
  )
  @ValueChooserModel(TaskTypeChooserValues.class)
  public TaskType taskType = TaskType.CHANGE_EXISTING_OBJECT;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Object",
    description = "Path to object that will be worked on",
    group = "#0",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = { RecordEL.class },
    displayPosition = 20
  )
  public String objectPath;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.TEXT,
    label = "Content",
    description = "Content that should be stored inside the newly created object.",
    dependencies = {
      @Dependency(configName = "taskType", triggeredByValues = "CREATE_NEW_OBJECT")
    },
    group = "#0",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = { RecordEL.class, TimeNowEL.class },
    displayPosition = 30
  )
  public String content;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MAP,
    label = "Tags",
    description = "Tags that should be added to given object.",
    dependencies = {
      @Dependency(configName = "taskType", triggeredByValues = "CHANGE_EXISTING_OBJECT")
    },
    group = "#0",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    elDefs = { RecordEL.class, TimeNowEL.class },
    displayPosition = 40
  )
  public Map<String, String> tags = Collections.emptyMap();
}
