/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.destination.kafka;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvHeaderChooserValues;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvModeChooserValues;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.JsonModeChooserValues;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringEL;

import java.util.Map;

@StageDef(
  version = 1,
  label = "Kafka Producer",
  description = "Writes data to Kafka",
  icon = "kafka.png")
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class KafkaDTarget extends DTarget {

  private static final String KAFKA_PRODUCER_OPTIONS_DEFAULT =
    "{" +
      " \"queue.buffering.max.ms\" : \"5000\", " +
      " \"message.send.max.retries\" : \"10\", " +
      " \"retry.backoff.ms\" : \"1000\" " +
    "}";

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "localhost:9092",
    label = "Broker URI",
    description = "Comma-separated list of URIs for brokers that write to the topic.  Use the format " +
      "<HOST>:<PORT>. To ensure a connection, enter as many as possible.",
    displayPosition = 10,
    group = "KAFKA"
  )
  public String metadataBrokerList;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Runtime Topic Resolution",
    description = "Select topic at runtime based on the field values in the record",
    displayPosition = 15,
    group = "KAFKA"
  )
  public boolean runtimeTopicResolution;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "${record:value('/topic')}",
    label = "Topic Expression",
    description = "An expression that resolves to the name of the topic to use",
    displayPosition = 20,
    elDefs = {RecordEL.class},
    group = "KAFKA",
    evaluation = ConfigDef.Evaluation.EXPLICIT,
    dependsOn = "runtimeTopicResolution",
    triggeredByValue = "true"
  )
  public String topicExpression;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.TEXT,
    lines = 5,
    defaultValue = "*",
    label = "Topic White List",
    description = "A comma-separated list of valid topic names. " +
      "Records with invalid topic names are treated as error records. " +
      "'*' indicates that all topic names are allowed.",
    displayPosition = 23,
    group = "KAFKA",
    dependsOn = "runtimeTopicResolution",
    triggeredByValue = "true"
  )
  public String topicWhiteList;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "topicName",
    label = "Topic",
    description = "",
    displayPosition = 25,
    group = "KAFKA",
    dependsOn = "runtimeTopicResolution",
    triggeredByValue = "false"
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
  @ValueChooserModel(PartitionStrategyChooserValues.class)
  public PartitionStrategy partitionStrategy;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    defaultValue = "${0}",
    label = "Partition Expression",
    description = "Determines the partition key to use with default kafka partitioner class in case of 'Default Partition Strategy'. In case of 'Expression Partition Strategy' it determines the partition number",
    displayPosition = 40,
    group = "KAFKA",
    dependsOn = "partitionStrategy",
    triggeredByValue = {"EXPRESSION", "DEFAULT"},
    elDefs = {RecordEL.class},
    evaluation = ConfigDef.Evaluation.EXPLICIT
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
  @ValueChooserModel(ProducerDataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "UTF-8",
    label = "Messages Charset",
    displayPosition = 51,
    group = "KAFKA",
    dependsOn = "dataFormat",
    triggeredByValue = {"TEXT", "JSON", "DELIMITED", "XML", "LOG"}
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String charset;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "One Message per Batch",
      description = "Generates a single Kafka message with all records in the batch",
      displayPosition = 55,
      group = "KAFKA"
  )
  public boolean singleMessagePerBatch;

  /********  For DELIMITED Content  ***********/

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MODEL,
    defaultValue = "CSV",
    label = "Delimiter Format",
    description = "",
    displayPosition = 100,
    group = "DELIMITED",
    dependsOn = "dataFormat",
    triggeredByValue = "DELIMITED"
  )
  @ValueChooserModel(CsvModeChooserValues.class)
  public CsvMode csvFileFormat;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MAP,
    defaultValue = KAFKA_PRODUCER_OPTIONS_DEFAULT,
    label = "Kafka Configuration",
    description = "Additional Kafka properties to pass to the underlying Kafka producer",
    displayPosition = 60,
    group = "KAFKA"
  )
  public Map<String, String> kafkaProducerConfigs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NO_HEADER",
      label = "Header Line",
      description = "",
      displayPosition = 110,
      group = "DELIMITED",
      dependsOn = "dataFormat",
      triggeredByValue = "DELIMITED"
  )
  @ValueChooserModel(CsvHeaderChooserValues.class)
  public CsvHeader csvHeader;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Remove New Line Characters",
      description = "Replaces new lines characters with white spaces",
      displayPosition = 120,
      group = "DELIMITED",
      dependsOn = "dataFormat",
      triggeredByValue = "DELIMITED"
  )
  public boolean csvReplaceNewLines;

  /********  For JSON *******/

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "MULTIPLE_OBJECTS",
      label = "JSON Content",
      description = "",
      displayPosition = 200,
      group = "JSON",
      dependsOn = "dataFormat",
      triggeredByValue = "JSON"
  )
  @ValueChooserModel(JsonModeChooserValues.class)
  public JsonMode jsonMode;

  /********  For TEXT Content  ***********/

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "/",
    label = "Text Field Path",
    description = "Field to write data to Kafka",
    displayPosition = 120,
    group = "TEXT",
    dependsOn = "dataFormat",
    triggeredByValue = "TEXT",
    elDefs = {StringEL.class}
  )
  @FieldSelectorModel(singleValued = true)
  public String textFieldPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Empty Line If No Text",
      description = "",
      displayPosition = 310,
      group = "TEXT",
      dependsOn = "dataFormat",
      triggeredByValue = "TEXT"
  )
  public boolean textEmptyLineIfNull;

  /********  For AVRO Content  ***********/

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.TEXT,
    defaultValue = "",
    label = "Avro Schema",
    description = "Optionally use the runtime:loadResource function to use a schema stored in a file",
    displayPosition = 320,
    group = "AVRO",
    dependsOn = "dataFormat",
    triggeredByValue = {"AVRO"},
    mode = ConfigDef.Mode.JSON
  )
  public String avroSchema;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "true",
    label = "Include Schema",
    description = "Includes Avro schema in the kafka message",
    displayPosition = 330,
    group = "AVRO",
    dependsOn = "dataFormat",
    triggeredByValue = "AVRO"
  )
  public boolean includeSchema;

  /********  For Binary Content  ***********/

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "/",
    label = "Binary Field Path",
    description = "Field to write data to Kafka",
    displayPosition = 120,
    group = "BINARY",
    dependsOn = "dataFormat",
    triggeredByValue = "BINARY",
    elDefs = {StringEL.class}
  )
  @FieldSelectorModel(singleValued = true)
  public String binaryFieldPath;

  @Override
  protected Target createTarget() {
    return new KafkaTarget(metadataBrokerList, runtimeTopicResolution, topic, topicExpression, topicWhiteList,
      partitionStrategy, partition, dataFormat, charset, singleMessagePerBatch, kafkaProducerConfigs, csvFileFormat,
      csvHeader, csvReplaceNewLines, jsonMode, textFieldPath, textEmptyLineIfNull,
      avroSchema, includeSchema, binaryFieldPath);
  }
}
