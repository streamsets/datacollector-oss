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
package com.streamsets.pipeline.stage.origin.tcp;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowDataParserFactory;
import com.streamsets.pipeline.lib.parser.net.netflow.OutputValuesMode;
import com.streamsets.pipeline.lib.parser.net.netflow.OutputValuesModeChooserValues;
import com.streamsets.pipeline.lib.parser.net.syslog.SyslogFramingMode;
import com.streamsets.pipeline.lib.parser.net.syslog.SyslogFramingModeChooserValues;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import java.util.List;

public class TCPServerSourceConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      displayPosition = 1,
      group = "DATA_FORMAT",
      dependsOn = "tcpMode",
      triggeredByValue = {"DELIMITED_RECORDS", "CHARACTER_BASED_LENGTH_FIELD", "FLUME_AVRO_IPC"}
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDefBean(groups = "DATA_FORMAT")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfigBean = new TlsConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Port",
      defaultValue = "[\"9999\"]",
      description = "Port to listen on",
      group = "TCP",
      displayPosition = 1,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public List<String> ports; // string so we can listen on multiple ports in the future

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Bind Address",
      defaultValue = "0.0.0.0",
      description = "Bind address for opening listener.",
      dependsOn = "tcpMode",
      triggeredByValue = "FLUME_AVRO_IPC",
      group = "TCP",
      displayPosition = 4,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String bindAddress;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Enable Native Transports (Epoll)",
      description = "Enable epoll transports.  Multithreaded performance will be significantly higher with this" +
          " option. Only available on 64-bit Linux systems",
      defaultValue = "false",
      group = "TCP",
      displayPosition = 5,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public boolean enableEpoll;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Number of Receiver Threads",
      description = "Number of receiver threads for each port. It should be based on the CPU cores expected to be" +
          " dedicated to the pipeline",
      defaultValue = "1",
      min = 1,
      group = "TCP",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public int numThreads;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "TCP Mode",
      description = "The mode the TCP server operates in, based on the expected input data format",
      defaultValue = "SYSLOG",
      group = "TCP",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(TCPModeChooserValues.class)
  public TCPMode tcpMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Syslog Message Transfer Framing Mode",
      description = "The TCP syslog message transfer mode to be used, as defined in RFC 6587.  Method change is not" +
          " allowed (i.e. must be consistent between all clients and sessions).",
      defaultValue = "OCTET_COUNTING",
      group = "SYSLOG",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "tcpMode",
      triggeredByValue = "SYSLOG"
  )
  @ValueChooserModel(SyslogFramingModeChooserValues.class)
  public SyslogFramingMode syslogFramingMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Non-transparent-framing Separator",
      description = "When using non-transparent-framing, this is the separator character that will appear between" +
          " separate syslog messages.  Specify using Java Unicode syntax (\"\\uxxxx\").  Defaults to line feed (000A).",
      defaultValue = "\\u000A",
      group = "SYSLOG",
      dependsOn = "syslogFramingMode",
      triggeredByValue = "NON_TRANSPARENT_FRAMING",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String nonTransparentFramingSeparatorCharStr;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTF-8",
      label = "Charset",
      description = "The character encoding that Syslog messages will have",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "SYSLOG",
      dependsOn = "tcpMode",
      triggeredByValue = "SYSLOG"
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String syslogCharset;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Record separator",
      description = "When using delimited records mode, this is the separator character that will appear between" +
          " separate records.  Specify using Java Unicode syntax (\"\\uxxxx\").  Defaults to line feed (000A).",
      defaultValue = "\\u000A",
      group = "TCP",
      dependsOn = "tcpMode",
      triggeredByValue = "DELIMITED_RECORDS",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String recordSeparatorStr;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTF-8",
      label = "Charset",
      description = "The character encoding that the character data with length prefix messages use. Note that the" +
          " length digits themselves, plus space, must be in a single byte encoding.",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "TCP",
      dependsOn = "tcpMode",
      triggeredByValue = "CHARACTER_BASED_LENGTH_FIELD"
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String lengthFieldCharset;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = NetflowDataParserFactory.DEFAULT_OUTPUT_VALUES_MODE_STR,
      label = NetflowDataParserFactory.OUTPUT_VALUES_MODE_LABEL,
      description = NetflowDataParserFactory.OUTPUT_VALUES_MODE_TOOLTIP,
      displayPosition = 90,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "NETFLOW_V9",
      dependsOn = "tcpMode",
      triggeredByValue = "NETFLOW"
  )
  @ValueChooserModel(OutputValuesModeChooserValues.class)
  public OutputValuesMode netflowOutputValuesMode = NetflowDataParserFactory.DEFAULT_OUTPUT_VALUES_MODE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = NetflowDataParserFactory.DEFAULT_MAX_TEMPLATE_CACHE_SIZE_STR,
      label = NetflowDataParserFactory.MAX_TEMPLATE_CACHE_SIZE_LABEL,
      description = NetflowDataParserFactory.MAX_TEMPLATE_CACHE_SIZE_TOOLTIP,
      displayPosition = 92,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "NETFLOW_V9",
      dependsOn = "tcpMode",
      triggeredByValue = "NETFLOW"
  )
  public int maxTemplateCacheSize = NetflowDataParserFactory.DEFAULT_MAX_TEMPLATE_CACHE_SIZE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = NetflowDataParserFactory.DEFAULT_TEMPLATE_CACHE_TIMEOUT_MS_STR,
      label = NetflowDataParserFactory.TEMPLATE_CACHE_TIMEOUT_MS_LABEL,
      description = NetflowDataParserFactory.TEMPLATE_CACHE_TIMEOUT_MS_TOOLTIP,
      displayPosition = 95,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "NETFLOW_V9",
      dependsOn = "tcpMode",
      triggeredByValue = "NETFLOW"
  )
  public int templateCacheTimeoutMs = NetflowDataParserFactory.DEFAULT_TEMPLATE_CACHE_TIMEOUT_MS;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Size (messages)",
      group = "TCP",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      max = Integer.MAX_VALUE
  )
  public int batchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Batch Wait Time (ms)",
      description = "Max time to wait for data before sending a batch",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "TCP",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxWaitTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "4096",
      label = "Max Message Size (bytes)",
      description = "Max message size in bytes",
      displayPosition = 150,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "TCP",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxMessageSize = 4096;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTF-8",
      label = "Charset",
      description = "The character set to be used when sending ack messages back to client",
      displayPosition = 200,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "TCP"
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String ackMessageCharset = "UTF-8";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTC",
      label = "Ack Time Zone",
      description = "Time zone to use for ack message evaluation (if time or time now ELs are used)",
      displayPosition = 210,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "TCP"
  )
  @ValueChooserModel(TimeZoneChooserValues.class)
  public String timeZoneID = "UTC";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      label = "Record Processed Ack Message",
      description = "Acknowledgement message to be sent back to the client upon each successfully processed record.",
      displayPosition = 250,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "TCP",
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String recordProcessedAckMessage;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      label = "Batch Completed Ack Message",
      description = "Acknowledgement message to be sent back to the client upon each successfully completed batch." +
          " The record in the EL context is the last record processed in the batch.",
      displayPosition = 260,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "TCP",
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String batchCompletedAckMessage;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "300",
      label = "Read Timeout (seconds)",
      description = "Period of time a connection can be idle. After that time, the connection is closed",
      displayPosition = 300,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "TCP",
      min = 1,
      max = 3600
  )
  public int readTimeout;
}
