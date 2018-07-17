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
package com.streamsets.pipeline.stage.destination.kinesis;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisConfigBean;

import java.util.Map;

public class KinesisProducerConfigBean extends KinesisConfigBean {

  @ConfigDefBean(groups = "DATA_FORMAT")
  public DataGeneratorFormatConfig dataFormatConfig;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      description = "Data format to use when receiving records from Kinesis",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "RANDOM",
      label = "Partitioning Strategy",
      description = "Partitioning strategy for partition key generation",
      displayPosition = 40,
      group = "#0"
  )
  @ValueChooserModel(PartitionStrategyChooserValues.class)
  public PartitionStrategy partitionStrategy;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "${0}",
      label = "Partition Expression",
      description = "EL that is used to evaluate the partitionKey when producing messages.",
      displayPosition = 45,
      group = "#0",
      dependsOn = "partitionStrategy",
      triggeredByValue = "EXPRESSION",
      elDefs = {RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String partitionExpression;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Kinesis Producer Configuration",
      description = "Additional Kinesis Producer properties to set.",
      displayPosition = 50,
      group = "#0"
  )
  public Map<String, String> producerConfigs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Preserve Record Order",
      defaultValue = "false",
      description = "Enabling this option will guarantee records will be written in-order to the stream. However, " +
          "enabling this option will disable batching and comes at the expense of throughput.",
      displayPosition = 60,
      group =  "#0"
  )
  public boolean preserveOrdering;
}
