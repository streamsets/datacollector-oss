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
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.config.PostProcessingOptionsChooserValues;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import java.util.List;

public class FileTailConfigBean {

  @ConfigDefBean(groups = "FILES")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      description = "The data format in the files (IMPORTANT: if Log, Log4j files with stack traces are not handled)",
      displayPosition = 1,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Pattern for Multiline",
      defaultValue = "",
      description = "RegEx pattern to detect main lines for Text and Log files with multi-line elements. " +
          "Use only if required as it impacts reading performance",
      displayPosition = 15,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "DATA_FORMAT",
      dependsOn = "dataFormat",
      triggeredByValue = { "TEXT", "LOG" }
  )
  public String multiLineMainPattern;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Maximum Batch Size",
      description = "Max number of lines that will be sent in a single batch",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "FILES",
      min = 0,
      max = Integer.MAX_VALUE
  )
  public int batchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5",
      label = "Batch Wait Time (secs)",
      description = " Maximum amount of time to wait to fill a batch before sending it",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "FILES",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxWaitTimeSecs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "File to Tail",
      description = "",
      displayPosition  = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FILES",
      elDefs = PatternEL.class
  )
  @ListBeanModel
  public List<FileInfo> fileInfos;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Allow Late Directories",
      description = "Enables reading from late-arriving directories." +
          " When enabled, the origin does not validate configured paths.",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "FILES",
      defaultValue = "false"
  )
  public boolean allowLateDirectories = false;

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
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Archive Directory",
      description = "Directory to archive files after they have been processed",
      displayPosition = 200,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "POST_PROCESSING",
      dependsOn = "postProcessing",
      triggeredByValue = "ARCHIVE"
  )
  public String archiveDir;
}
