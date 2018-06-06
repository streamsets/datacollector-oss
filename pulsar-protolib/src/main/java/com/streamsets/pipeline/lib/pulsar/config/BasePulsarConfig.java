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

package com.streamsets.pipeline.lib.pulsar.config;

import com.streamsets.pipeline.api.ConfigDef;

import java.util.HashMap;
import java.util.Map;

public class BasePulsarConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Pulsar URL",
      description = "Pulsar service URL. Example: http://localhost:8080 or pulsar://localhost:6650",
      displayPosition = 10,
      defaultValue = "http://localhost:8080",
      group = "PULSAR"
  )
  public String serviceURL;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      label = "Pulsar keep alive interval",
      description = "How often to check whether the connections are still alive. Put time in seconds",
      displayPosition = 30,
      defaultValue = "30",
      min = 0,
      max = 60,
      group = "PULSAR"
  )
  public int keepAliveInterval = 30;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      label = "Pulsar operation timeout",
      description = "Pulsar Producer-create, Consumer-subscribe and Consumer-unsubscribe operations will be retried " +
          "until this interval, after which the operation will be marked as failed.",
      displayPosition = 40,
      defaultValue = "30",
      min = 0,
      max = 60,
      group = "PULSAR"
  )
  public int operationTimeout = 30;

}
