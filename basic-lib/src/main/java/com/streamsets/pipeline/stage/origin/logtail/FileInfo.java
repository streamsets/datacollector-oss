/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooser;
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
  @ValueChooser(FileRollModeChooserValues.class)
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
