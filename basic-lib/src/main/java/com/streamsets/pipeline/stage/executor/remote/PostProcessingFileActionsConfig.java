/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.executor.remote;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.config.WholeFileExistsActionChooserValues;
import com.streamsets.pipeline.lib.el.RecordEL;

/**
 * Various actions that can be performed on the given file.
 */
public class PostProcessingFileActionsConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "File Name Expression",
      description = "File Name Expression",
      defaultValue = "${record:value('/filepath')}",
      group = "ACTION",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class}
  )
  public String filePath;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Task",
    description = "Task to be performed",
    defaultValue = "DELETE_FILE",
    group = "ACTION",
    displayPosition = 20,
    displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(PostProcessingFileActionChooserValues.class)
  public PostProcessingFileAction actionType = PostProcessingFileAction.DELETE_FILE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Target Directory",
      description = "Directory to move the file to. Enter a location relative to the configured user's FTP/SFTP root",
      group = "ACTION",
      dependsOn = "actionType",
      triggeredByValue = {"MOVE_FILE"},
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String targetDir;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "File Exists Action",
      description = "Action to perform if file already exists in the target directory",
      defaultValue = "OVERWRITE",
      group = "ACTION",
      dependsOn = "actionType",
      triggeredByValue = {"MOVE_FILE"},
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(WholeFileExistsActionChooserValues.class)
  public WholeFileExistsAction fileExistsAction = WholeFileExistsAction.OVERWRITE;
}
