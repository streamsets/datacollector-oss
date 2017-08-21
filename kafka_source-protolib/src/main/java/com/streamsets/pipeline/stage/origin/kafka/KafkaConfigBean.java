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
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.KafkaOriginGroups;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.Utils.KAFKA_CONFIG_BEAN_PREFIX;

public class KafkaConfigBean {

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
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "localhost:9092",
      label = "Broker URI",
      description = "Comma-separated list of Kafka brokers. Use format <HOST>:<PORT>",
      displayPosition = 20,
      group = "KAFKA"
  )
  public String metadataBrokerList;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "localhost:2181",
      label = "ZooKeeper URI",
      description = "Comma-separated list of ZooKeepers followed by optional chroot path. Use format: <HOST1>:<PORT1>,<HOST2>:<PORT2>,<HOST3>:<PORT3>/<ital><CHROOT_PATH></ital>",
      displayPosition = 30,
      group = "KAFKA"
  )
  public String zookeeperConnect;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "streamsetsDataCollector",
      label = "Consumer Group",
      displayPosition = 40,
      group = "KAFKA"
  )
  public String consumerGroup;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "topicName",
      label = "Topic",
      displayPosition = 50,
      group = "KAFKA"
  )
  public String topic;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Produce Single Record",
      description = "Generates a single record for multiple objects within a message",
      displayPosition = 60,
      group = "KAFKA"
  )
  public boolean produceSingleRecordPerMessage;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Size (records)",
      description = "Max number of records per batch (Standalone only)",
      displayPosition = 70,
      group = "KAFKA",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxBatchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2000",
      label = "Batch Wait Time (ms)",
      description = "Max time to wait for data before sending a partial or empty batch",
      displayPosition = 80,
      group = "KAFKA",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxWaitTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Rate Limit Per Partition (Kafka messages)",
      description = "Max number of messages to read per batch per partition(Cluster Mode only)",
      displayPosition = 85,
      group = "KAFKA",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxRatePerPartition;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Key Deserializer",
      description = "Method used to deserialize the Kafka message key. Set to Confluent when the Avro schema ID is embedded in each message.",
      defaultValue = "STRING",
      dependsOn = "dataFormat",
      triggeredByValue = "AVRO",
      displayPosition = 90,
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
      displayPosition = 100,
      group = "KAFKA"
  )
  @ValueChooserModel(ValueDeserializerChooserValues.class)
  public Deserializer valueDeserializer = Deserializer.DEFAULT;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "",
      label = "Kafka Configuration",
      description = "Additional Kafka properties to pass to the underlying Kafka consumer",
      displayPosition = 120,
      group = "KAFKA"
  )
  public Map<String, String> kafkaConsumerConfigs = new HashMap<>();

  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    if (keyDeserializer == Deserializer.CONFLUENT || valueDeserializer == Deserializer.CONFLUENT) {
      try {
        getClass().getClassLoader().loadClass(Deserializer.CONFLUENT.getKeyClass());
      } catch (ClassNotFoundException ignored) { // NOSONAR
        issues.add(
            context.createConfigIssue(
                KafkaOriginGroups.KAFKA.name(),
                KAFKA_CONFIG_BEAN_PREFIX + "keyDeserializer",
                KafkaErrors.KAFKA_44
            )
        );
      }
      if (dataFormatConfig.schemaRegistryUrls == null || dataFormatConfig.schemaRegistryUrls.isEmpty()) {
        issues.add(
            context.createConfigIssue(
                KafkaOriginGroups.DATA_FORMAT.name(),
                KAFKA_CONFIG_BEAN_PREFIX + "dataFormatConfig.schemaRegistryUrls",
                KafkaErrors.KAFKA_43
            )
        );
      }
    }
  }
}
