/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kinesis;

import com.amazonaws.regions.Regions;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.configurablestage.DSourceOffsetCommitter;
import com.streamsets.pipeline.stage.lib.kinesis.AWSRegionChooserValues;

@StageDef(
    version = "1.0.0",
    label = "Kinesis Consumer",
    description = "Reads data from Kinesis",
    icon = "kinesis.png"
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class KinesisDSource extends DSourceOffsetCommitter {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "US_WEST_2",
      label = "Endpoint",
      displayPosition = 10,
      group = "KINESIS"
  )
  @ValueChooser(AWSRegionChooserValues.class)
  public Regions region;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Stream Name",
      displayPosition = 20,
      group = "KINESIS"
  )
  public String streamName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "SDC_JSON",
      label = "Input Record Format",
      description = "Record format to use when receiving records from Kinesis",
      displayPosition = 30,
      group = "KINESIS"
  )
  @ValueChooser(InputRecordFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "500",
      label = "Max Batch Size (messages)",
      description = "Max number of records per batch. Kinesis will not return more than 2MB/s/shard.",
      displayPosition = 40,
      group = "KINESIS",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxBatchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Read Interval (ms)",
      description = "Time KCL should wait between requests per shard. Cannot be set below 200ms. >250ms recommended.",
      displayPosition = 50,
      group = "KINESIS",
      min = 200,
      max = Integer.MAX_VALUE
  )
  public long idleTimeBetweenReads;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Batch Wait Time (ms)",
      description = "Max time to wait for data before sending a batch",
      displayPosition = 50,
      group = "KINESIS",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public long maxWaitTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "60000",
      label = "Preview Batch Wait Time (ms)",
      description = "Max time to wait for data for preview mode. This should be at least several seconds for Kinesis.",
      displayPosition = 60,
      group = "KINESIS",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public long previewWaitTime;

  /** Authentication Options */

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "AWS Access Key ID",
      displayPosition = 70,
      group = "KINESIS"
  )
  public String awsAccessKeyId;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "AWS Secret Access Key",
      displayPosition = 80,
      group = "KINESIS"
  )
  public String awsSecretAccessKey;

  @Override
  protected Source createSource() {
    return new KinesisSource(
        region,
        streamName,
        dataFormat,
        maxBatchSize,
        idleTimeBetweenReads,
        maxWaitTime,
        previewWaitTime,
        awsAccessKeyId,
        awsSecretAccessKey
    );
  }
}
