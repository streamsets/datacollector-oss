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
package com.streamsets.pipeline.lib.dirspooler;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.config.PostProcessingOptionsChooserValues;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

public class SpoolDirConfigBean {

  @ConfigDefBean(groups = "FILES")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      description = "Format of data in the files",
      displayPosition = 1,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Files Directory",
      description = "Use a local directory",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FILES"
  )
  public String spoolDir;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1",
      label = "Number of Threads",
      description = "Number of parallel threads that read data",
      displayPosition = 11,
      group = "FILES",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 1
  )
  public int numberOfThreads = 1;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "File Name Pattern Mode",
      description = "Select whether the File Name Pattern specified uses glob pattern syntax or regex syntax.",
      defaultValue = "GLOB",
      displayPosition = 15,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "FILES"
  )
  @ValueChooserModel(PathMatcherModeChooserValues.class)
  public PathMatcherMode pathMatcherMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "File Name Pattern",
      description = "A glob or regular expression that defines the pattern of the file names in the directory.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FILES"
  )
  public String filePattern;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "LEXICOGRAPHICAL",
      label = "Read Order",
      description = "Read files based on the last-modified timestamp or lexicographically ascending file names. When timestamp ordering is used, files with the same timestamp are ordered based on file names.",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "FILES"
  )
  @ValueChooserModel(FileOrderingChooseValues.class)
  public FileOrdering useLastModified = FileOrdering.LEXICOGRAPHICAL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Process Subdirectories",
      description = "Process files in subdirectories of Files Directory.  " +
          "Only file names matching File Name Pattern will be processed.",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "useLastModified",
      triggeredByValue = "TIMESTAMP",
      group = "FILES"
  )
  public boolean processSubdirectories;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Allow Late Directory",
      description = "Enables reading from a late-arriving directory." +
          " When enabled, the origin does not validate the configured path.",
      displayPosition = 50,
      group = "FILES",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      defaultValue = "false"
  )
  public boolean allowLateDirectory = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Buffer Limit (KB)",
      defaultValue = "128",
      description = "Low level reader buffer limit to avoid out of Memory errors",
      displayPosition = 70,
      group = "FILES",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int overrunLimit;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Batch Size (recs)",
      defaultValue = "1000",
      description = "Max number of records per batch",
      displayPosition = 43,
      group = "FILES",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      max = Integer.MAX_VALUE
  )
  public int batchSize;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "60",
      label = "Batch Wait Time (secs)",
      description = "Max time to wait for new files before sending an empty batch",
      displayPosition = 48,
      group = "FILES",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 1
  )
  public long poolingTimeoutSecs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Files Soft Limit",
      description = "Maximum number of files added to the processing queue at one time. " +
          "This is a soft limit and can be temporarily exceeded.",
      displayPosition = 60,
      group = "FILES",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxSpoolFiles;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5",
      label = "Spooling Period (secs)",
      description = "Max time period to spool the files",
      displayPosition = 61,
      group = "FILES",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 1,
      max = Integer.MAX_VALUE
  )
  public long spoolingPeriod = 5;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "First File to Process",
      description = "When configured, the Data Collector does not process earlier (naturally ascending order) file names",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FILES"
  )
  public String initialFileToProcess;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Error Directory",
      description = "Directory for files that could not be fully processed",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "POST_PROCESSING"
  )
  public String errorArchiveDir;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NONE",
      label = "File Post Processing",
      description = "Action to take after processing a file",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "POST_PROCESSING"
  )
  @ValueChooserModel(PostProcessingOptionsChooserValues.class)
  public PostProcessingOptions postProcessing;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Archive Directory",
      description = "Directory to archive files after they have been processed",
      displayPosition = 200,
      group = "POST_PROCESSING",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "postProcessing",
      triggeredByValue = "ARCHIVE"
  )
  public String archiveDir;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "Archive Retention Time (mins)",
      description = "How long archived files should be kept before deleting, a value of zero means forever",
      displayPosition = 210,
      group = "POST_PROCESSING",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "postProcessing",
      triggeredByValue = "ARCHIVE",
      min = 0
  )
  public long retentionTimeMins;
}
