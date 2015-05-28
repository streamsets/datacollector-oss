/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooser;

public class FileInfo {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Tag",
      description = "Metadata tag",
      displayPosition = 5,
      group = "FILE"
  )
  public String tag;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Directory",
      description = "Directory path for the file to tail",
      displayPosition = 10,
      group = "FILE"
  )
  public String dirName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "REVERSE_COUNTER",
      label = "File Naming",
      description = "",
      displayPosition = 20,
      group = "FILE"
  )
  @ValueChooser(RolledFilesModeChooserValues.class)
  public FilesRollMode fileRollMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Active File",
      description = "Name of the file to tail",
      displayPosition = 30,
      group = "FILE",
      dependsOn = "fileRollMode",
      triggeredByValue = {
          "REVERSE_COUNTER", "DATE_YYYY_MM", "DATE_YYYY_MM_DD", "DATE_YYYY_MM_DD_HH",
          "DATE_YYYY_MM_DD_HH_MM", "DATE_YYYY_WW", "ALPHABETICAL"
      }
  )
  public String file;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = ".*",
      label = "Periodic File Pattern",
      description = "A Java regular expression matching the expected file names",
      displayPosition = 40,
      group = "FILE",
      dependsOn = "fileRollMode",
      triggeredByValue = "PERIODIC"
  )
  public String periodicFileRegEx;

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
