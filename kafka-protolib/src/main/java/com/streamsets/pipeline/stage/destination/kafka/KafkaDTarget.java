/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.kafka;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvModeChooserValues;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.configurablestage.DTarget;

import java.util.List;
import java.util.Map;

@GenerateResourceBundle
@StageDef(
  version = "1.0.0",
  label = "Kafka Producer",
  description = "Writes data to Kafka",
  icon = "kafka.png")
@ConfigGroups(value = Groups.class)
public class KafkaDTarget extends DTarget {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "localhost:9092",
    label = "Broker URIs",
    description = "Comma-separated list of URIs for brokers that write to the topic.  Use the format " +
      "<HOST>:<PORT>. To ensure a connection, enter as many as possible.",
    displayPosition = 10,
    group = "KAFKA"
  )
  public String metadataBrokerList;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "topicName",
    label = "Topic",
    description = "",
    displayPosition = 20,
    group = "KAFKA"
  )
  public String topic;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "ROUND_ROBIN",
    label = "Partition Strategy",
    description = "Strategy to select a partition to write to",
    displayPosition = 30,
    group = "KAFKA"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = PartitionStrategyChooserValues.class)
  public PartitionStrategy partitionStrategy;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.EL_NUMBER,
    defaultValue = "${0}",
    label = "Partition Expression",
    description = "Expression that determines the partition to write to",
    displayPosition = 40,
    group = "KAFKA",
    dependsOn = "partitionStrategy",
    triggeredByValue = "EXPRESSION"
  )
  public String partition;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "SDC_JSON",
    label = "Data Format",
    description = "",
    displayPosition = 50,
    group = "KAFKA"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = ProducerDataFormatChooserValues.class)
  public DataFormat payloadType;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MAP,
    defaultValue = "",
    label = "Kafka Configuration",
    description = "Additional Kafka properties to pass to the underlying Kafka producer",
    displayPosition = 60,
    group = "KAFKA"
  )
  public Map<String, String> kafkaProducerConfigs;

  /********  For DELIMITED Content  ***********/

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MODEL,
    defaultValue = "CSV",
    label = "Delimiter Format",
    description = "",
    displayPosition = 100,
    group = "DELIMITED",
    dependsOn = "payloadType",
    triggeredByValue = "DELIMITED"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CsvModeChooserValues.class)
  public CsvMode csvFileFormat;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "TEXT",
    label = "Fields",
    description = "Fields to write to Kafka",
    displayPosition = 110,
    group = "DELIMITED",
    dependsOn = "payloadType",
    triggeredByValue = "DELIMITED"
  )
  @FieldSelector
  public List<String> fieldPaths;

  /********  For TEXT Content  ***********/

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "/",
    label = "Field",
    description = "Field to write data to Kafka",
    displayPosition = 120,
    group = "TEXT",
    dependsOn = "payloadType",
    triggeredByValue = "TEXT"
  )
  @FieldSelector(singleValued = true)
  public String fieldPath;

  @Override
  protected Target createTarget() {
    return new KafkaTarget(metadataBrokerList, topic, partitionStrategy, partition, payloadType, kafkaProducerConfigs,
      csvFileFormat, fieldPaths, fieldPath);
  }
}
