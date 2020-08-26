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
package com.streamsets.pipeline.stage.origin.udptokafka;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DSourceOffsetCommitter;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;

@StageDef(
  version = 4,
  label = "UDP to Kafka",
  execution = ExecutionMode.STANDALONE,
  description = "Receives UDP packages and writes them to Kafka",
  icon="udptokafka.png",
  recordsByRef = true,
  upgrader = UDPToKafkaUpgrader.class,
  upgraderDef = "upgrader/UDPToKafkaDSource.yaml",
  onlineHelpRefUrl ="index.html?contextID=task_tvh_bhz_pw"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
@HideConfigs(
  value = {
      "kafkaTargetConfig.dataGeneratorFormatConfig.csvFileFormat",
      "kafkaTargetConfig.dataGeneratorFormatConfig.csvHeader",
      "kafkaTargetConfig.dataGeneratorFormatConfig.csvReplaceNewLines",
      "kafkaTargetConfig.dataGeneratorFormatConfig.csvReplaceNewLinesString",
      "kafkaTargetConfig.dataGeneratorFormatConfig.csvCustomDelimiter",
      "kafkaTargetConfig.dataGeneratorFormatConfig.csvCustomEscape",
      "kafkaTargetConfig.dataGeneratorFormatConfig.csvCustomQuote",
      "kafkaTargetConfig.dataGeneratorFormatConfig.jsonMode",
      "kafkaTargetConfig.dataGeneratorFormatConfig.textFieldPath",
      "kafkaTargetConfig.dataGeneratorFormatConfig.textEmptyLineIfNull",
      "kafkaTargetConfig.dataGeneratorFormatConfig.textRecordSeparator",
      "kafkaTargetConfig.dataGeneratorFormatConfig.avroSchemaSource",
      "kafkaTargetConfig.dataGeneratorFormatConfig.avroSchema",
      "kafkaTargetConfig.dataGeneratorFormatConfig.schemaRegistryUrls",
      "kafkaTargetConfig.dataGeneratorFormatConfig.schemaLookupMode",
      "kafkaTargetConfig.dataGeneratorFormatConfig.subject",
      "kafkaTargetConfig.dataGeneratorFormatConfig.subjectToRegister",
      "kafkaTargetConfig.dataGeneratorFormatConfig.schemaRegistryUrlsForRegistration",
      "kafkaTargetConfig.dataGeneratorFormatConfig.basicAuthUserInfo",
      "kafkaTargetConfig.dataGeneratorFormatConfig.basicAuthUserInfoForRegistration",
      "kafkaTargetConfig.dataGeneratorFormatConfig.registerSchema",
      "kafkaTargetConfig.dataGeneratorFormatConfig.schemaId",
      "kafkaTargetConfig.dataGeneratorFormatConfig.includeSchema",
      "kafkaTargetConfig.dataGeneratorFormatConfig.avroCompression",
      "kafkaTargetConfig.dataGeneratorFormatConfig.binaryFieldPath",
      "kafkaTargetConfig.dataGeneratorFormatConfig.protoDescriptorFile",
      "kafkaTargetConfig.dataGeneratorFormatConfig.messageType",
      "kafkaTargetConfig.dataGeneratorFormatConfig.fileNameEL",
      "kafkaTargetConfig.dataGeneratorFormatConfig.wholeFileExistsAction",
      "kafkaTargetConfig.dataGeneratorFormatConfig.includeChecksumInTheEvents",
      "kafkaTargetConfig.dataGeneratorFormatConfig.checksumAlgorithm",
      "kafkaTargetConfig.dataFormat",
      "kafkaTargetConfig.runtimeTopicResolution",
      "kafkaTargetConfig.partitionStrategy",
      "kafkaTargetConfig.partition",
      "kafkaTargetConfig.singleMessagePerBatch",
      "kafkaTargetConfig.topicExpression",
      "kafkaTargetConfig.topicWhiteList"
  }
)
public class UDPToKafkaDSource extends DSourceOffsetCommitter {

  @ConfigDefBean
  public UDPConfigBean udpConfigs;

  @ConfigDefBean()
  public KafkaTargetConfig kafkaTargetConfig;

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
    kafkaTargetConfig.topic = topic;
    return new UDPToKafkaSource(udpConfigs, kafkaTargetConfig);
  }
}
