/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.FileRawSourcePreviewer;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.configurablestage.DSourceOffsetCommitter;

@GenerateResourceBundle
@StageDef(
    version="1.0.0",
    label="File Tail",
    description = "Reads log or JSON data as it is written to a file",
    icon="fileTail.png"
)
@RawSource(rawSourcePreviewer = FileRawSourcePreviewer.class)
@ConfigGroups(Groups.class)
public class FileTailDSource extends DSourceOffsetCommitter {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      description = "The data format in the files",
      displayPosition = 10,
      group = "FILE"
  )
  @ValueChooser(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "File Path",
      description = "Full file path of the file to tail",
      displayPosition = 20,
      group = "FILE"
  )
  public String fileName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10",
      label = "Maximum Lines per Batch",
      description = "The maximum number of file lines that will be sent in a single batch",
      displayPosition = 30,
      group = "FILE",
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
      displayPosition = 40,
      group = "FILE",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxWaitTimeSecs;

  @Override
  protected Source createSource() {
    return new FileTailSource(dataFormat, fileName, batchSize, maxWaitTimeSecs);
  }

}
