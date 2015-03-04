/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvHeaderChooserValues;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvModeChooserValues;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.DataFormatChooserValues;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.JsonModeChooserValues;
import com.streamsets.pipeline.configurablestage.DSourceOffsetCommitter;

import java.util.Map;

@GenerateResourceBundle
@StageDef(
    version = "1.0.0",
    label = "Kafka Consumer",
    description = "Reads data from Kafka",
    icon = "kafka.png"
)
@RawSource(rawSourcePreviewer = KafkaRawSourcePreviewer.class, mimeType = "application/json")
@ConfigGroups(value = Groups.class)
public class KafkaDSource extends DSourceOffsetCommitter {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "localhost:2181",
    label = "ZooKeeper Connection String",
    description = "Comma-separated ist of the Zookeeper <HOST>:<PORT> used by the Kafka brokers",
    displayPosition = 10,
    group = "KAFKA"
  )
  public String zookeeperConnect;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "streamsetsDataCollector",
    label = "Consumer Group",
    description = "Pipeline consumer group",
    displayPosition = 20,
    group = "KAFKA"
  )
  public String consumerGroup;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "topicName",
    label = "Topic",
    description = "",
    displayPosition = 30,
    group = "KAFKA"
  )
  public String topic;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Data Format",
    description = "",
    displayPosition = 40,
    group = "KAFKA"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.INTEGER,
    defaultValue = "1000",
    label = "Max Batch Size (messages)",
    description = "",
    displayPosition = 50,
    group = "KAFKA"
  )
  public int maxBatchSize;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.INTEGER,
    defaultValue = "1000",
    label = "Batch Wait Time (millisecs)",
    description = "Max time to wait for data before sending a batch",
    displayPosition = 60,
    group = "KAFKA"
  )
  public int maxWaitTime;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MAP,
    defaultValue = "",
    label = "Kafka Configuration",
    description = "Additional Kafka properties to pass to the underlying Kafka consumer",
    displayPosition = 70,
    group = "KAFKA"
  )
  public Map<String, String> kafkaConsumerConfigs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "MULTIPLE_OBJECTS",
      label = "JSON Content",
      description = "",
      displayPosition = 100,
      group = "JSON",
      dependsOn = "dataFormat",
      triggeredByValue = "JSON"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = JsonModeChooserValues.class)
  public JsonMode jsonContent;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Produce Single Record",
    description = "Generates a single record for multiple objects within a message",
    displayPosition = 5,
    group = "KAFKA"
  )
  public boolean produceSingleRecord;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "CSV",
      label = "Delimiter Format Type",
      description = "",
      displayPosition = 200,
      group = "DELIMITED",
      dependsOn = "dataFormat",
      triggeredByValue = "DELIMITED"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CsvModeChooserValues.class)
  public CsvMode csvFileFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NO_HEADER",
      label = "Header Line",
      description = "",
      displayPosition = 210,
      group = "DELIMITED",
      dependsOn = "dataFormat",
      triggeredByValue = "DELIMITED"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = CsvHeaderChooserValues.class)
  public CsvHeader csvHeader;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Delimiter Element",
      description = "XML element that acts as a record delimiter",
      displayPosition = 300,
      group = "XML",
      dependsOn = "dataFormat",
      triggeredByValue = "XML"
  )
  public String xmlRecordElement;

  @Override
  protected Source createSource() {
    return new KafkaSource(zookeeperConnect, consumerGroup, topic, dataFormat, maxBatchSize,
      maxWaitTime, kafkaConsumerConfigs, jsonContent, produceSingleRecord, csvFileFormat, csvHeader,
      xmlRecordElement);
  }

}
