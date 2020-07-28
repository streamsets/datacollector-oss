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
package com.streamsets.pipeline.stage.origin.remote;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.config.PostProcessingOptionsChooserValues;
import com.streamsets.pipeline.lib.remote.RemoteConfigBean;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

public class RemoteDownloadConfigBean {

  @ConfigDefBean(groups = "REMOTE")
  public BasicConfig basic = new BasicConfig();

  @ConfigDefBean(groups = "REMOTE")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDefBean(groups = {"REMOTE","CREDENTIALS"})
  public RemoteConfigBean remoteConfig = new RemoteConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      group = "DATA_FORMAT",
      displayPosition = 1
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Archive on error",
      description = "On error, should the file be archive to a local directory",
      group = "ERROR",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public boolean archiveOnError;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Archive Directory",
      description = "Directory to archive files, if an irrecoverable error is encountered",
      group = "ERROR",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "archiveOnError",
      triggeredByValue = "true"
  )
  public String errorArchiveDir = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Process Subdirectories",
      description = "Process files in subdirectories of the specified path",
      group = "REMOTE",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public boolean processSubDirectories;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "File Name Pattern Mode",
      description = "Select whether the File Name Pattern specified uses glob pattern syntax or regex syntax.",
      defaultValue = "GLOB",
      displayPosition = 35,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "REMOTE"
  )
  @ValueChooserModel(FilePatternModeChooserValues.class)
  public FilePatternMode filePatternMode;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "File Name Pattern",
      defaultValue = "*",
      description =  "A glob or regular expression that defines the pattern of the file names in the directory" +
          " (Glob '*' selects all files). Files are processed in chronological order.",
      group = "REMOTE",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String filePattern;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "First File to Process",
      description = "When configured, the Data Collector does not process earlier file names",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "REMOTE"
  )
  public String initialFileToProcess;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "File Processing Delay",
      description = "Milliseconds to wait before a new file is processed",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "REMOTE"
  )
  public long processingDelay;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NONE",
      label = "File Post Processing",
      description = "Action to take after processing a file",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "POST_PROCESSING",
      dependsOn = "dataFormat",
      triggeredByValue = {  // Everything in DataFormat other than "WHOLE_FILE"
          "TEXT",
          "JSON",
          "DELIMITED",
          "XML",
          "SDC_JSON",
          "LOG",
          "AVRO",
          "BINARY",
          "PROTOBUF",
          "DATAGRAM",
          "SYSLOG",
          "NETFLOW",
          "EXCEL",
          "FLOWFILE",
      }
  )
  @ValueChooserModel(PostProcessingOptionsChooserValues.class)
  public PostProcessingOptions postProcessing;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Archive Directory",
      description = "Directory to archive files after they have been processed",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "POST_PROCESSING",
      dependsOn = "postProcessing",
      triggeredByValue = "ARCHIVE"
  )
  public String archiveDir;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Path Relative to User Home Directory",
      description = "If checked, the Archive Directory path is resolved relative to the logged in user's home " +
          "directory, if a username is entered in the Credentials tab or in the URL.",
      displayPosition = 120,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "POST_PROCESSING",
      dependsOn = "postProcessing",
      triggeredByValue = "ARCHIVE"
  )
  public boolean archiveDirUserDirIsRoot = true;

}
