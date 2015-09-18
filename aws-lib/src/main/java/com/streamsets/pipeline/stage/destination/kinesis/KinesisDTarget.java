/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.kinesis;

import com.amazonaws.regions.Regions;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.stage.lib.kinesis.AWSRegionChooserValues;

@StageDef(
    version = 1,
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
      displayPosition = 10,
      group = "KINESIS"
  )
  @ValueChooserModel(AWSRegionChooserValues.class)
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
      label = "Data Format",
      description = "Data format to use when publishing to Kinesis",
      displayPosition = 30,
      group = "KINESIS"
  )
  @ValueChooserModel(OutputRecordFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "ROUND_ROBIN",
      label = "Partitioning Strategy",
      description = "Partitioning strategy for partition key generation",
      displayPosition = 40,
      group = "KINESIS"
  )
  @ValueChooserModel(PartitionStrategyChooserValues.class)
  public PartitionStrategy partitionStrategy;

  /** Authentication Options */

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "AWS Access Key ID",
      displayPosition = 50,
      group = "KINESIS"
  )
  public String awsAccessKeyId;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "AWS Secret Access Key",
      displayPosition = 60,
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
