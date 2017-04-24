/*
 * Copyright 2017 StreamSets Inc.
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

package com.streamsets.pipeline.stage.origin.tcp;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.DataFormatChooserValues;
import com.streamsets.pipeline.lib.parser.net.syslog.SyslogFramingMode;
import com.streamsets.pipeline.lib.parser.net.syslog.SyslogFramingModeChooserValues;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import com.streamsets.pipeline.lib.tls.TlsConnectionType;
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
      triggeredByValue = "DELIMITED_RECORDS"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDefBean(groups = "DATA_FORMAT")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfigBean = new TlsConfigBean(TlsConnectionType.SERVER);

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Port",
      defaultValue = "[\"9999\"]",
      description = "Port to listen on",
      group = "TCP",
      displayPosition = 1
  )
  public List<String> ports; // string so we can listen on multiple ports in the future

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Enable Multithreading",
      description = "Use multiple receiver threads for each port. Only available on 64-bit Linux systems",
      defaultValue = "false",
      group = "TCP",
      displayPosition = 5
  )
  public boolean enableEpoll;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Use TLS",
      description = "Enable TLS on the TCP transport.  Must specify the X.509 certificate chain file, the private" +
          " key file (in PKCS8 format), and a key encryption passphrase (if used for the key).",
      displayPosition = 10,
      group = "TCP"
  )
  public boolean tlsEnabled;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Number of Receiver Threads",
      description = "Number of receiver threads for each port. It should be based on the CPU cores expected to be" +
          " dedicated to the pipeline",
      defaultValue = "1",
      group = "TCP",
      displayPosition = 20
  )
  public int numThreads;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "TCP Mode",
      description = "The mode the TCP server operates in, based on the expected input data format",
      defaultValue = "SYSLOG",
      group = "TCP",
      displayPosition = 30
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
      dependsOn = "tcpMode",
      triggeredByValue = "SYSLOG"
  )
  @ValueChooserModel(SyslogFramingModeChooserValues.class)
  public SyslogFramingMode syslogFramingMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Non-transparent-framing separator",
      description = "When using non-transparent-framing, this is the separator character that will appear between" +
          " separate syslog messages.  Specify using Java Unicode syntax (\"\\uxxxx\").  Defaults to line feed (000A).",
      defaultValue = "\\u000A",
      group = "SYSLOG",
      dependsOn = "syslogFramingMode",
      triggeredByValue = "NON_TRANSPARENT_FRAMING",
      displayPosition = 20
  )
  public String nonTransparentFramingSeparatorCharStr;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTF-8",
      label = "Charset",
      displayPosition = 30,
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
      displayPosition = 60
  )
  public String recordSeparatorStr;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Size (messages)",
      group = "TCP",
      displayPosition = 70,
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
      displayPosition = 80,
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
      group = "TCP",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxMessageSize = 4096;
}
