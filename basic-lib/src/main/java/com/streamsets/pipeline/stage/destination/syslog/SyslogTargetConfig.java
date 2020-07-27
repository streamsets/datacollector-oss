/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.syslog;

import com.cloudbees.syslog.MessageFormat;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

public class SyslogTargetConfig {

  @ConfigDef(
      required = true,
      label = "Protocol",
      type = ConfigDef.Type.MODEL,
      defaultValue = "UDP",
      description = "Syslog protocol",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SYSLOG"
  )
  @ValueChooserModel(ProtocolChooserValues.class)
  public String protocol;

  @ConfigDef(
      required = true,
      label = "Syslog Host",
      type = ConfigDef.Type.STRING,
      defaultValue = "localhost",
      description = "Syslog server hostname",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SYSLOG"
  )
  public String serverName;

  @ConfigDef(
      required = true,
      label = "Syslog Port",
      type = ConfigDef.Type.NUMBER,
      defaultValue = "514",
      description = "Syslog port",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SYSLOG"
  )
  public int serverPort;

  @ConfigDef(
      required = true,
      label = "Message Format",
      type = ConfigDef.Type.MODEL,
      defaultValue = "RFC_5424",
      description = "Message format to use for messages sent over UDP. TCP messages will use RFC 6587 regardless of " +
          "this setting.",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SYSLOG",
      dependsOn = "protocol",
      triggeredByValue = "UDP"
  )
  @ValueChooserModel(MessageFormatChooserValues.class)
  public MessageFormat messageFormat = MessageFormat.RFC_5424;

  @ConfigDef(
      required = false,
      label = "Socket Connection Timeout (ms)",
      type = ConfigDef.Type.NUMBER,
      defaultValue = "500",
      description = "TCP socket connection timeout in milliseconds",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "SYSLOG",
      dependsOn = "protocol",
      triggeredByValue = "TCP"
  )
  public int timeout = 500;

  @ConfigDef(
      required = false,
      label = "TCP Connection Retries",
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2",
      description = "Number of times to retry a TCP connection",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "SYSLOG",
      dependsOn = "protocol",
      triggeredByValue = "TCP"
  )
  public int retries = 2;

  @ConfigDef(
      required = true,
      label = "Enable SSL",
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      description = "Enable SSL for TCP connections",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SYSLOG",
      dependsOn = "protocol",
      triggeredByValue = "TCP"
  )
  public boolean useSsl;

  @ConfigDef(
      required = true,
      label = "Timestamp",
      type = ConfigDef.Type.STRING,
      defaultValue = "${time:now()}",
      description = "Expression to get the message timestamp. Default is the current system time.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MESSAGE",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String timestampEL = "${time:now()}";

  @ConfigDef(
      required = true,
      label = "Hostname",
      type = ConfigDef.Type.STRING,
      defaultValue = "localhost",
      description = "Expression to get the messsage hostname",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MESSAGE",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String hostnameEL;

  @ConfigDef(
      required = true,
      label = "Severity Level",
      type = ConfigDef.Type.STRING,
      defaultValue = "6",
      description = "Expression to get the severity level integer. Default is 6 (Informational).",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MESSAGE",
      elDefs = RecordEL.class,
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String severityEL;

  @ConfigDef(
      required = true,
      label = "Syslog Facility",
      type = ConfigDef.Type.STRING,
      defaultValue = "1",
      description = "Expression to get the syslog facility integer. Default is 1 (User).",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MESSAGE",
      elDefs = RecordEL.class,
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String facilityEL;

  @ConfigDef(
      required = false,
      label = "Application Name",
      type = ConfigDef.Type.STRING,
      description = "Expression to get the application name",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MESSAGE",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String appNameEL = "";

  @ConfigDef(
      required = false,
      label = "Message ID",
      type = ConfigDef.Type.STRING,
      description = "Expression to get the message ID",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MESSAGE",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String messageIdEL = "";

  @ConfigDef(
      required = false,
      label = "Process ID",
      type = ConfigDef.Type.STRING,
      description = "Expression to get the process ID",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MESSAGE",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String processIdEL = "";
}
