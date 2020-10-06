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

package com.streamsets.pipeline.stage.pubsub.origin;

import com.streamsets.pipeline.api.ConfigDef;

public class PubSubAdvancedConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Number of Subscribers",
      description = "Can be configured to spawn multiple Subscribers",
      defaultValue = "1",
      min = 1,
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public int numSubscribers;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Subscriber Thread Pool Size",
      description = "Size of the thread pool per Subscriber.",
      defaultValue = "1",
      min = 1,
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public int numThreadsPerSubscriber;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Custom Endpoint",
      description = "Optional <host>:<port> formatted endpoint, for example to test against a Pub/Sub emulator.",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public String customEndpoint;
}
