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
package com.streamsets.pipeline.lib.rabbitmq.config;

import com.rabbitmq.client.ConnectionFactory;
import com.streamsets.pipeline.api.ConfigDef;

public class RabbitAdvancedConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Automatic Recovery Enabled",
      defaultValue = "true",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public boolean automaticRecoveryEnabled = true;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Network Recovery Interval",
      description = "How long automatic recovery will wait before attempting to reconnect, in ms",
      defaultValue = "5000",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      group = "#0"
  )
  public int networkRecoveryInterval = 5000;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Connection Timeout (ms)",
      defaultValue = "0",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      group = "#0"
  )
  public int connectionTimeout = ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Handshake Timeout (ms)",
      defaultValue = "10000",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      group = "#0"
  )
  public int handshakeTimeout = ConnectionFactory.DEFAULT_HANDSHAKE_TIMEOUT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Shutdown Timeout (ms)",
      defaultValue = "10000",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      group = "#0"
  )
  public int shutdownTimeout = ConnectionFactory.DEFAULT_SHUTDOWN_TIMEOUT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Heartbeat Timeout (secs)",
      description = "Heartbeat timeout in seconds. Zero disables heartbeats",
      defaultValue = "0",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      group = "#0"
  )
  public int heartbeatInterval = ConnectionFactory.DEFAULT_HEARTBEAT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Maximum Frame Size (bytes)",
      description = "Zero for no limit",
      defaultValue = "0",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      group = "#0"
  )
  public int frameMax = ConnectionFactory.DEFAULT_FRAME_MAX;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Maximum Channel Number",
      description = "Zero for no limit",
      defaultValue = "0",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      group = "#0"
  )
  public int channelMax = ConnectionFactory.DEFAULT_CHANNEL_MAX;
}
