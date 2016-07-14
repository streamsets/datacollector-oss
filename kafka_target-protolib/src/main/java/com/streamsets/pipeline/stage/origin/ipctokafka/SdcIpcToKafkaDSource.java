/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.ipctokafka;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DSourceOffsetCommitter;
import com.streamsets.pipeline.stage.destination.kafka.KafkaConfigBean;

@StageDef(
  version = 1,
  label = "SDC RPC to Kafka",
  execution = ExecutionMode.STANDALONE,
  description = "Receives records via SDC RPC from a Data Collector pipeline that uses an SDC RPC destination and " +
    "writes them to Kafka",
  icon="sdcipctokafka.png",
  onlineHelpRefUrl = "TBD"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
@HideConfigs(
    value = {
      "kafkaConfigBean.dataGeneratorFormatConfig.csvFileFormat",
      "kafkaConfigBean.dataGeneratorFormatConfig.csvHeader",
      "kafkaConfigBean.dataGeneratorFormatConfig.csvReplaceNewLines",
      "kafkaConfigBean.dataGeneratorFormatConfig.csvReplaceNewLinesString",
      "kafkaConfigBean.dataGeneratorFormatConfig.csvCustomDelimiter",
      "kafkaConfigBean.dataGeneratorFormatConfig.csvCustomEscape",
      "kafkaConfigBean.dataGeneratorFormatConfig.csvCustomQuote",
      "kafkaConfigBean.dataGeneratorFormatConfig.jsonMode",
      "kafkaConfigBean.dataGeneratorFormatConfig.textFieldPath",
      "kafkaConfigBean.dataGeneratorFormatConfig.textEmptyLineIfNull",
      "kafkaConfigBean.dataGeneratorFormatConfig.avroSchemaInHeader",
      "kafkaConfigBean.dataGeneratorFormatConfig.avroSchema",
      "kafkaConfigBean.dataGeneratorFormatConfig.includeSchema",
      "kafkaConfigBean.dataGeneratorFormatConfig.avroCompression",
      "kafkaConfigBean.dataGeneratorFormatConfig.binaryFieldPath",
      "kafkaConfigBean.dataGeneratorFormatConfig.protoDescriptorFile",
      "kafkaConfigBean.dataGeneratorFormatConfig.messageType",
      "kafkaConfigBean.dataFormat",
      "kafkaConfigBean.kafkaConfig.runtimeTopicResolution",
      "kafkaConfigBean.kafkaConfig.partitionStrategy",
      "kafkaConfigBean.kafkaConfig.partition",
      "kafkaConfigBean.kafkaConfig.singleMessagePerBatch",
      "kafkaConfigBean.kafkaConfig.topicExpression",
      "kafkaConfigBean.kafkaConfig.topicWhiteList"
    }
)
public class SdcIpcToKafkaDSource extends DSourceOffsetCommitter {

  @ConfigDefBean
  public RpcConfigs configs;

  @ConfigDefBean()
  public KafkaConfigBean kafkaConfigBean;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "900",
    label = "Max Message Size (KB)",
    description = "Maximum size of the message written to Kafka. Configure in relationship to Max Batch Request Size and the maximum message size configured in Kafka. ",
    displayPosition = 30,
    group = "KAFKA",
    min = 1,
    max = 100
  )
  public int kafkaMaxMessageSize;

  // The "topic" config from KafkaConfigBean depends on "runtimeTopicResolution" config which is hidden because
  // it does not make sense in this origin. As a result "topic" config will also not be shown to the user.
  // The only caveat is that the UI will not be able to pin point the topic configuration if it fails validation.
  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "topicName",
    label = "Topic",
    description = "",
    displayPosition = 25,
    group = "KAFKA"
  )
  public String topic;

  @Override
  protected Source createSource() {
    kafkaConfigBean.kafkaConfig.topic = topic;
    return new SdcIpcToKafkaSource(configs, kafkaConfigBean, kafkaMaxMessageSize);
  }
}
