/**
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
package com.streamsets.pipeline.stage.origin.multikafka;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.kafka.KafkaAutoOffsetReset;
import com.streamsets.pipeline.lib.kafka.KafkaAutoOffsetResetValues;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MultiKafkaBeanConfig {
  @ConfigDefBean(groups = "KAFKA")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      description = "Format of data in the topic",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "localhost:9092",
      label = "Broker URI",
      description = "Comma-separated list of Kafka brokers. Use format <HOST>:<PORT>",
      displayPosition = 10,
      group = "KAFKA"
  )
  public String brokerURI = "localhost:9092";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "streamsetsDataCollector",
      label = "Consumer Group",
      description = "Consumer Group",
      displayPosition = 20,
      group = "KAFKA"
  )
  public String consumerGroup;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      defaultValue = "[\"myTopic\"]",
      label = "Topic List",
      description = "List of topics to consume",
      displayPosition = 30,
      group = "KAFKA"
  )
  public List<String> topicList = new ArrayList<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Produce Single Record",
      description = "Generates a single record for multiple objects within a message",
      displayPosition = 40,
      group = "KAFKA"
  )
  public boolean produceSingleRecordPerMessage;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1",
      label = "Number of Threads",
      description = "Number of threads to allocate for consuming all topics",
      displayPosition = 50,
      group = "KAFKA"
  )
  public int numberOfThreads;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Size (records)",
      description = "Maximum number of records per batch",
      displayPosition = 60,
      group = "KAFKA"
  )
  public int maxBatchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2000",
      label = "Batch Wait Time (ms)",
      description = "Max time to wait for data before sending a partial or empty batch",
      displayPosition = 70,
      group = "KAFKA"
  )
  public int batchWaitTime;

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
  @ValueChooserModel(KafkaAutoOffsetResetValues.class)
  public KafkaAutoOffsetReset kafkaAutoOffsetReset;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Auto Offset Reset Timestamp (ms)",
      description = "Date in milliseconds from which to start consuming messages. Default is January 1st, 1970",
      defaultValue = "0",
      dependsOn = "kafkaAutoOffsetReset",
      triggeredByValue = "TIMESTAMP",
      displayPosition = 90,
      group = "KAFKA",
      min = 0
  )
  public long timestampToSearchOffsets;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "",
      label = "Configuration properties",
      description = "Additional Kafka properties to pass to the underlying Kafka consumer",
      displayPosition = 100,
      group = "KAFKA"
  )
  public Map<String, String> kafkaOptions;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Key Deserializer",
      description = "Method used to deserialize the Kafka message key. Set to Confluent when the Avro schema ID is embedded in each message.",
      defaultValue = "STRING",
      dependsOn = "dataFormat",
      triggeredByValue = "AVRO",
      displayPosition = 110,
      group = "KAFKA"
  )
  @ValueChooserModel(KeyDeserializerChooserValues.class)
  public Deserializer keyDeserializer = Deserializer.STRING;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Value Deserializer",
      description = "Method used to deserialize the Kafka message value. Set to Confluent when the Avro schema ID is embedded in each message.",
      defaultValue = "DEFAULT",
      dependsOn = "dataFormat",
      triggeredByValue = "AVRO",
      displayPosition = 120,
      group = "KAFKA"
  )
  @ValueChooserModel(ValueDeserializerChooserValues.class)
  public Deserializer valueDeserializer = Deserializer.DEFAULT;

  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {

  }
}
