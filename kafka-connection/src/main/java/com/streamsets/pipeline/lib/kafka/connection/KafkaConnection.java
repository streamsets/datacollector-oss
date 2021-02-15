/*
 * Copyright 2020 StreamSets Inc.
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

package com.streamsets.pipeline.lib.kafka.connection;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
@ConnectionDef(
    label = "Kafka",
    type = KafkaConnection.TYPE,
    description = "Connects to Kafka",
    version = 2,
    upgraderDef = "upgrader/KafkaConnectionUpgrader.yaml",
    supportedEngines = { ConnectionEngine.COLLECTOR, ConnectionEngine.TRANSFORMER }
)
@ConfigGroups(KafkaConnectionGroups.class)
public class KafkaConnection {

  public static final String TYPE = "STREAMSETS_KAFKA";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "localhost:9092",
      label = "Broker URI",
      description = "Comma-separated list of Kafka brokers. Use format <HOST>:<PORT>",
      displayPosition = 10,
      group = "KAFKA"
  )
  public String metadataBrokerList = "localhost:9092";

  @ConfigDefBean(groups = "SECURITY")
  public KafkaSecurityConfig securityConfig = new KafkaSecurityConfig();
}
