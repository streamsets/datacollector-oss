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
package com.streamsets.pipeline.stage.origin.udp;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.configurablestage.DSource;
import com.streamsets.pipeline.lib.parser.udp.ParserConfig;
import com.streamsets.pipeline.config.DatagramMode;
import com.streamsets.pipeline.config.DatagramModeChooserValues;

import java.util.List;

import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.AUTH_FILE_PATH;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.CHARSET;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.CONVERT_TIME;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.EXCLUDE_INTERVAL;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.TYPES_DB_PATH;

@StageDef(
    version = 2,
    label = "UDP Source",
    description = "Listens for UDP messages on a single port",
    icon = "udp.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    upgrader = UDPSourceUpgrader.class,
    onlineHelpRefUrl = "index.html#Origins/UDP.html#task_kgn_rcv_1s"
)

@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class UDPDSource extends DSource {
  private ParserConfig parserConfig = new ParserConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Port",
      defaultValue = "[\"9995\"]",
      description = "Port to listen on",
      group = "UDP",
      displayPosition = 10
  )
  public List<String> ports; // string so we can listen on multiple ports in the future

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Enable Multithreading",
      description = "Use multiple receiver threads for each port. Only available on 64-bit Linux systems",
      defaultValue = "false",
      group = "UDP",
      displayPosition = 15
  )
  public boolean enableEpoll;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Number of Receiver Threads",
      description = "Number of receiver threads for each port. It should be based on the CPU cores expected to be dedicated to the pipeline",
      defaultValue = "1",
      group = "UDP",
      dependsOn = "enableEpoll",
      triggeredByValue = "true",
      displayPosition = 16
  )
  public int numThreads;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      defaultValue = "SYSLOG",
      group = "UDP",
      displayPosition = 20
  )
  @ValueChooserModel(DatagramModeChooserValues.class)
  public DatagramMode dataFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Size (messages)",
      group = "UDP",
      displayPosition = 30,
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
      displayPosition = 40,
      group = "UDP",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxWaitTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTF-8",
      label = "Charset",
      displayPosition = 5,
      group = "SYSLOG",
      dependsOn = "dataFormat",
      triggeredByValue = "SYSLOG"
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String syslogCharset;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "TypesDB File Path",
      description = "User-specified TypesDB file. Overrides the included version.",
      displayPosition = 10,
      group = "COLLECTD",
      dependsOn = "dataFormat",
      triggeredByValue = "COLLECTD"
  )
  public String typesDbPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Convert Hi-Res Time & Interval",
      description = "Converts high resolution time format interval and timestamp to unix time in (ms).",
      displayPosition = 20,
      group = "COLLECTD",
      dependsOn = "dataFormat",
      triggeredByValue = "COLLECTD"
  )
  public boolean convertTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Exclude Interval",
      description = "Excludes the interval field from output records.",
      displayPosition = 30,
      group = "COLLECTD",
      dependsOn = "dataFormat",
      triggeredByValue = "COLLECTD"
  )
  public boolean excludeInterval;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Auth File",
      description = "",
      displayPosition = 40,
      group = "COLLECTD",
      dependsOn = "dataFormat",
      triggeredByValue = "COLLECTD"
  )
  public String authFilePath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTF-8",
      label = "Charset",
      displayPosition = 50,
      group = "COLLECTD",
      dependsOn = "dataFormat",
      triggeredByValue = "COLLECTD"
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String collectdCharset;

  @Override
  protected Source createSource() {
    Utils.checkNotNull(dataFormat, "Data format cannot be null");
    Utils.checkNotNull(ports, "Ports cannot be null");

    switch (dataFormat) {
      case SYSLOG:
        parserConfig.put(CHARSET, syslogCharset);
        break;
      case COLLECTD:
        parserConfig.put(CHARSET, collectdCharset);
        break;
      default:
        // NOOP
    }

    parserConfig.put(CONVERT_TIME, convertTime);
    parserConfig.put(TYPES_DB_PATH, typesDbPath);
    parserConfig.put(EXCLUDE_INTERVAL, excludeInterval);
    parserConfig.put(AUTH_FILE_PATH, authFilePath);

    // Force single thread if epoll not enabled.
    if (!enableEpoll) {
      numThreads = 1;
    }
    return new UDPSource(ports, enableEpoll, numThreads, parserConfig, dataFormat, batchSize, maxWaitTime);
  }
}
