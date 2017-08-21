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
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.FileRollMode;
import com.streamsets.pipeline.config.FileRollModeChooserValues;

public class FileInfo {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Tag",
      description = "Metadata tag",
      displayPosition = 60,
      group = "FILE"
  )
  public String tag;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Path",
      description = "Full path of the file to tail. If using 'Files matching a pattern' as file naming you must use " +
                    "'" + PatternEL.TOKEN + "' token in the file name of the file path.",
      displayPosition = 10,
      group = "FILE",
      elDefs = PatternEL.class
  )
  public String fileFullPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "REVERSE_COUNTER",
      label = "Naming",
      description = "",
      displayPosition = 20,
      group = "FILE"
  )
  @ValueChooserModel(FileRollModeChooserValues.class)
  public FileRollMode fileRollMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = ".*",
      label = "Pattern",
      description = "A Java regular expression to match the '" + PatternEL.TOKEN + "' section in the file name",
      displayPosition = 40,
      group = "FILE",
      dependsOn = "fileRollMode",
      triggeredByValue = "PATTERN"
  )
  public String patternForToken;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "First File",
      description = "First file to process. Leave empty for all.",
      displayPosition = 50,
      group = "FILE"
  )
  public String firstFile;

}
