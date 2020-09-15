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
package com.streamsets.pipeline.stage.origin.maprstreams;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.StageDef;

import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.kafka.KafkaAutoOffsetReset;
import com.streamsets.pipeline.lib.kafka.KafkaAutoOffsetResetValues;
import com.streamsets.pipeline.stage.origin.multikafka.MultiKafkaDSource;
import com.streamsets.pipeline.stage.origin.multikafka.MultiKafkaRawSourcePreviewer;

@StageDef(
    version = 7,
    label = "MapR Multitopic Streams Consumer",
    description = "Reads data from multiple topics of a MapR streams",
    execution = ExecutionMode.STANDALONE,
    icon = "mapr_es.png",
    recordsByRef = true,
    upgraderDef = "upgrader/MultiMapRStreamsDSource.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_pkc_lww_lbb"
)
@RawSource(rawSourcePreviewer = MultiKafkaRawSourcePreviewer.class,  mimeType = "*/*")
@HideConfigs({
  "conf.connectionConfig.connection.metadataBrokerList",
  "conf.kafkaAutoOffsetReset"
})
@GenerateResourceBundle
public class MultiMapRStreamsDSource extends MultiKafkaDSource {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Auto Offset Reset",
      description = "Strategy to select the position to start consuming messages from the Kafka partition when no " +
          "offset is currently saved",
      defaultValue = "EARLIEST",
      displayPosition = 80,
      group = "KAFKA"
  )
  @ValueChooserModel(KafkaMaprAutoOffsetResetValues.class)
  public KafkaMaprAutoOffsetReset kafkaMaprAutoOffsetReset;

  @Override
  protected PushSource createPushSource() {
    conf.kafkaAutoOffsetReset = KafkaAutoOffsetReset.valueOf(kafkaMaprAutoOffsetReset.name());
    return super.createPushSource();
  }
}
