/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.flume;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvHeaderChooserValues;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvModeChooserValues;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.JsonModeChooserValues;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.lib.el.StringEL;

import java.util.Map;

@StageDef(
  version = "1.0.0",
  label = "Flume Destination",
  description = "Writes data to Flume Source",
  icon = "flume.png")
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class FlumeDTarget extends DTarget {

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MAP,
    defaultValue = "[ {\n" +
      "        \"key\" : \"h1\",\n" +
      "        \"value\" : \"localhost:41414\"\n" +
      "      } ]",
    label = "Flume Hosts Configuration",
    description = "Flume host alias and the address in the form 'host:port'",
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
  @ValueChooser(ClientTypeChooserValues.class)
  public ClientType clientType;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Backoff",
    description = "Indicates whether the client should temporarily blacklist a failed host",
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
    label = "Max Backoff (millisecs)",
    description = "Max timeout in millis that a client will remain inactive due to a previous failure with that host " +
      "(default: 0, which effectively becomes 30000)",
    displayPosition = 50,
    group = "FLUME",
    dependsOn = "clientType",
    triggeredByValue = "AVRO_LOAD_BALANCING"
  )
  public int maxBackOff;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MODEL,
    defaultValue = "ROUND_ROBIN",
    label = "Host Selection Strategy",
    description = "The host selection strategy used when load-balancing among hosts",
    displayPosition = 60,
    group = "FLUME",
    dependsOn = "clientType",
    triggeredByValue = "AVRO_LOAD_BALANCING"
  )
  @ValueChooser(HostSelectionStrategyChooserValues.class)
  public HostSelectionStrategy hostSelectionStrategy;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "100",
    label = "Batch Size",
    description = "",
    displayPosition = 70,
    group = "FLUME"
  )
  public int batchSize;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "20000",
    label = "Connection Timeout (millisecs)",
    description = "",
    displayPosition = 80,
    group = "FLUME"
  )
  public int connectionTimeout;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "20000",
    label = "Request Timeout (millisecs)",
    description = "",
    displayPosition = 90,
    group = "FLUME"
  )
  public int requestTimeout;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "5",
    label = "Max Retry Attempts",
    description = "Number of times to attempt resending data to flume agent in case of a failure.",
    displayPosition = 100,
    group = "FLUME"
  )
  public int maxRetryAttempts;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "120000", //2 minutes
    label = "Wait Between Retries in milliseconds",
    description = "The time sdc waits between attempts to resend data to flume agent in case of a failure.",
    displayPosition = 110,
    group = "FLUME"
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
  @ValueChooser(FlumeDestinationDataFormatChooserValues.class)
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
  @ValueChooser(CharsetChooserValues.class)
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
  @ValueChooser(CsvModeChooserValues.class)
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
  @ValueChooser(CsvHeaderChooserValues.class)
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
  @ValueChooser(JsonModeChooserValues.class)
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
    triggeredByValue = "TEXT",
    elDefs = {StringEL.class}
  )
  @FieldSelector(singleValued = true)
  public String textFieldPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Empty Line if no Text",
      description = "",
      displayPosition = 200,
      group = "TEXT",
      dependsOn = "dataFormat",
      triggeredByValue = "TEXT"
  )
  public boolean textEmptyLineIfNull;

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
      waitBetweenRetries
    );
  }
}
