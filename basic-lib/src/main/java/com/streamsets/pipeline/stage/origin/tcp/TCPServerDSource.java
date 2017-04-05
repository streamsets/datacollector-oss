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

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.configurablestage.DPushSource;
import com.streamsets.pipeline.lib.parser.udp.ParserConfig;

import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.AUTH_FILE_PATH;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.CHARSET;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.CONVERT_TIME;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.EXCLUDE_INTERVAL;
import static com.streamsets.pipeline.lib.parser.udp.ParserConfigKey.TYPES_DB_PATH;

@StageDef(
    version = 1,
    label = "TCP Server Source",
    description = "Listens for TCP messages on one or more ports",
    icon = "ethernet.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    upgrader = TCPServerSourceUpgrader.class,
    onlineHelpRefUrl = ""
)

@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class TCPServerDSource extends DPushSource {
  private ParserConfig parserConfig = new ParserConfig();

  @ConfigDefBean
  public TCPServerSourceConfig conf;

  @Override
  protected PushSource createPushSource() {
    Utils.checkNotNull(conf.tcpMode, "Data format cannot be null");
    Utils.checkNotNull(conf.ports, "Ports cannot be null");

    switch (conf.tcpMode) {
      case SYSLOG:
        parserConfig.put(CHARSET, conf.syslogCharset);
        break;
      case COLLECTD:
        parserConfig.put(CHARSET, conf.collectdCharset);
        break;
      default:
        // NOOP
    }

    parserConfig.put(CONVERT_TIME, conf.convertTime);
    parserConfig.put(TYPES_DB_PATH, conf.typesDbPath);
    parserConfig.put(EXCLUDE_INTERVAL, conf.excludeInterval);
    parserConfig.put(AUTH_FILE_PATH, conf.authFilePath);

    // Force single thread if epoll not enabled.
    if (!conf.enableEpoll) {
      conf.numThreads = 1;
    }
    // Force single thread if epoll not enabled.
    if (!conf.enableEpoll) {
      conf.numThreads = 1;
    }
    return new TCPServerSource(
        conf.maxMessageSize,
        conf.ports,
        conf.enableEpoll,
        conf.numThreads,
        conf.syslogCharset,
        conf.tcpMode,
        conf.syslogFramingMode,
        conf.nonTransparentFramingSeparatorCharStr,
        conf.batchSize,
        conf.maxWaitTime
    );
  }
}
