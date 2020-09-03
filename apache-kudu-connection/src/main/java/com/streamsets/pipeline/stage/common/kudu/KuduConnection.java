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
package com.streamsets.pipeline.stage.common.kudu;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
@ConnectionDef(
    label = "Kudu",
    type = KuduConnection.TYPE,
    description = "Connects to Kudu",
    version = 1,
    upgraderDef = "upgrader/KuduConnection.yaml",
    supportedEngines = {ConnectionEngine.COLLECTOR, ConnectionEngine.TRANSFORMER}
)
@ConfigGroups(KuduConnectionGroups.class)
public class KuduConnection {

  public static final String TYPE = "STREAMSETS_KUDU";

  // kudu tab
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Kudu Masters",
      description = "Comma-separated list of \"host:port\" pairs of the masters",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "KUDU"
  )
  public String kuduMaster;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      min = 0,
      label = "Maximum Number of Worker Threads",
      description = "Set the maximum number of worker threads. If set to 0, " +
          "the default (2 * the number of available processors) is used.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  public int numWorkers;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10000",
      label = "Operation Timeout (milliseconds)",
      min = 0,
      description = "Default timeout used for user operations (using sessions and scanners). A value of 0 disables the timeout.",
      displayPosition = 25,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  public int operationTimeout = 10000;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "30000",
      label = "Admin Operation Timeout (milliseconds)",
      min = 0,
      description = "Default timeout used for admin operations (openTable, getTableSchema, connectionRetry). " +
          "A value of 0 disables the timeout.",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  public int adminOperationTimeout = 30000;

}
