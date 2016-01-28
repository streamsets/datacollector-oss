/**
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
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.common.InterfaceAudience;
import com.streamsets.pipeline.common.InterfaceStability;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public class S3AdvancedConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Proxy",
      description = "Whether or not to connect to S3 through a proxy",
      defaultValue = "false",
      displayPosition = 10,
      group = "#0"
  )
  public boolean useProxy;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Proxy Host",
    description = "Optional proxy host the client will connect through",
    displayPosition = 20,
    dependsOn = "useProxy",
    triggeredByValue = "true",
    group = "#0"
  )
  public String proxyHost;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      label = "Proxy Port",
      description = "Optional proxy port the client will connect through",
      displayPosition = 30,
      dependsOn = "useProxy",
      triggeredByValue = "true",
      group = "#0"
  )
  public int proxyPort;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Proxy User",
      description = "Optional proxy user name to use if connecting through a proxy",
      displayPosition = 40,
      dependsOn = "useProxy",
      triggeredByValue = "true",
      group = "#0"
  )
  public String proxyUser;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Proxy Password",
      description = "Optional proxy password to use when connecting through a proxy",
      displayPosition = 50,
      dependsOn = "useProxy",
      triggeredByValue = "true",
      group = "#0"
  )
  public String proxyPassword;
}
