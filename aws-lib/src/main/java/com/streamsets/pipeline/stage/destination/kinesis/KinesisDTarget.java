/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.kinesis;

import com.amazonaws.regions.Regions;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.stage.lib.kinesis.AWSRegionChooserValues;

@StageDef(
    version = "1.0.0",
    label = "Kinesis Producer",
    description = "Writes data to Amazon Kinesis",
    icon = "kinesis.png")
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle

public class KinesisDTarget extends DTarget {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "US_WEST_2",
      label = "Endpoint",
      description = "The AWS region you wish to use.",
      displayPosition = 10,
      group = "KINESIS"
  )
  @ValueChooser(AWSRegionChooserValues.class)
  public Regions region;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Stream Name",
      description = "The name of the stream in Kinesis",
      displayPosition = 20,
      group = "KINESIS"
  )
  public String streamName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "SDC_JSON",
      label = "Output record format",
      description = "Record format to use when publishing to Kinesis.",
      displayPosition = 25,
      group = "KINESIS"
  )
  @ValueChooser(OutputRecordFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "Round Robin",
      label = "Partitioning Strategy",
      description = "Partitioning strategy for partition key generation.",
      displayPosition = 26,
      group = "KINESIS"
  )
  @ValueChooser(PartitionStrategyChooserValues.class)
  public PartitionStrategy partitionStrategy;

  /** Authentication Options */

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "AWS Access Key ID",
      description = "AWS Access Key ID in Kinesis with privileges to this Kinesis Stream",
      displayPosition = 30,
      group = "KINESIS"
  )
  public String awsAccessKeyId;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "AWS Secret Access Key",
      description = "AWS Secret Access Key with privileges to this Kinesis Stream",
      displayPosition = 40,
      group = "KINESIS"
  )
  public String awsSecretAccessKey;

  @Override
  protected Target createTarget() {
    return new KinesisTarget(
        region,
        streamName,
        dataFormat,
        partitionStrategy,
        awsAccessKeyId,
        awsSecretAccessKey
    );
  }
}
