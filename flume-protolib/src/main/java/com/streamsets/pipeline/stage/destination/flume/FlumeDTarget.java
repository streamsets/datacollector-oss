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
package com.streamsets.pipeline.stage.destination.flume;

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

import java.util.Map;

@StageDef(
  version = 1,
  label = "Flume",
  description = "Writes data to Flume Source",
  icon = "flume.png")
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class FlumeDTarget extends DTarget {

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MAP,
    defaultValue = "{ \"h1\" : \"localhost:41414\" }",
    label = "Hosts Configuration",
    description = "Flume host alias and the address in the form <HOST>:<PORT>",
    displayPosition = 10,
    group = "FLUME"
  )
  public Map<String, String> flumeHostsConfig;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "AVRO_FAILOVER",
    label = "Client Type",
    displayPosition = 20,
    group = "FLUME"
  )
  @ValueChooserModel(ClientTypeChooserValues.class)
  public ClientType clientType;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Backoff",
    description = "Temporarily avoid writing to a failed host",
    displayPosition = 40,
    group = "FLUME",
    dependsOn = "clientType",
    triggeredByValue = "AVRO_LOAD_BALANCING"
  )
  public boolean backOff;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "0",
    label = "Max Backoff (ms)",
    description = "Max ms that a client will remain inactive due to a previous failure with that host " +
      "(default: 0, which effectively becomes 30000)",
    displayPosition = 50,
    group = "FLUME",
    dependsOn = "clientType",
    triggeredByValue = "AVRO_LOAD_BALANCING",
    min = 0,
    max = Integer.MAX_VALUE
  )
  public int maxBackOff;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MODEL,
    defaultValue = "ROUND_ROBIN",
    label = "Host Selection Strategy",
    description = "Strategy used to load balance between hosts",
    displayPosition = 60,
    group = "FLUME",
    dependsOn = "clientType",
    triggeredByValue = "AVRO_LOAD_BALANCING"
  )
  @ValueChooserModel(HostSelectionStrategyChooserValues.class)
  public HostSelectionStrategy hostSelectionStrategy;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "100",
    label = "Flume Batch Size (events)",
    displayPosition = 70,
    group = "FLUME",
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int batchSize;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "20000",
    label = "Flume Client Connection Timeout (ms)",
    displayPosition = 80,
    group = "FLUME",
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int connectionTimeout;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "20000",
    label = "Flume Client Request Timeout (ms)",
    description = "",
    displayPosition = 90,
    group = "FLUME",
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int requestTimeout;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "5",
    label = "Max Retry Attempts",
    description = "Number of times to resend data to the Flume agent in case of failures",
    displayPosition = 100,
    group = "FLUME",
    min = 0,
    max = Integer.MAX_VALUE
  )
  public int maxRetryAttempts;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "120000", //2 minutes
    label = "Retry Wait Time (ms)",
    description = "Time to wait before resending data to Flume",
    displayPosition = 110,
    group = "FLUME",
    min = 1,
    max = Integer.MAX_VALUE
  )
  public long waitBetweenRetries;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "SDC_JSON",
    label = "Data Format",
    description = "",
    displayPosition = 120,
    group = "FLUME"
  )
  @ValueChooserModel(FlumeDestinationDataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "UTF-8",
    label = "Event Charset",
    displayPosition = 130,
    group = "FLUME",
    dependsOn = "dataFormat",
    triggeredByValue = {"TEXT", "JSON", "DELIMITED", "XML", "LOG"}
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String charset;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "One Event per Batch",
      description = "Generates a single Flume event with all records in the batch",
      displayPosition = 140,
      group = "FLUME"
  )
  public boolean singleEventPerBatch;

  /********  For DELIMITED Content  ***********/

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MODEL,
    defaultValue = "CSV",
    label = "Delimiter Format",
    description = "",
    displayPosition = 150,
    group = "DELIMITED",
    dependsOn = "dataFormat",
    triggeredByValue = "DELIMITED"
  )
  @ValueChooserModel(CsvModeChooserValues.class)
  public CsvMode csvFileFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NO_HEADER",
      label = "Header Line",
      description = "",
      displayPosition = 160,
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
      displayPosition = 170,
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
      displayPosition = 180,
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
    displayPosition = 190,
    group = "TEXT",
    dependsOn = "dataFormat",
    triggeredByValue = "TEXT"
  )
  @FieldSelectorModel(singleValued = true)
  public String textFieldPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Empty Line If No Text",
      description = "",
      displayPosition = 200,
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
    description = "Includes the Avro schema in the Flume event",
    displayPosition = 330,
    group = "AVRO",
    dependsOn = "dataFormat",
    triggeredByValue = "AVRO"
  )
  public boolean includeSchema;


  @Override
  protected Target createTarget() {
    return new FlumeTarget(
      flumeHostsConfig,
      dataFormat,
      charset,
      singleEventPerBatch,
      csvFileFormat,
      csvHeader,
      csvReplaceNewLines,
      jsonMode,
      textFieldPath,
      textEmptyLineIfNull,
      clientType,
      backOff,
      hostSelectionStrategy,
      maxBackOff,
      batchSize,
      connectionTimeout,
      requestTimeout,
      maxRetryAttempts,
      waitBetweenRetries,
      avroSchema,
      includeSchema
    );
  }
}
