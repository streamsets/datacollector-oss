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
package com.streamsets.pipeline.stage.lib.aws;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public class ProxyConfig {

  @ConfigDef(
      required = true,
      label = "Connection Timeout",
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10",
      description = "Set connection timeout (in seconds)",
      displayPosition = 4995,
      group = "#0",
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public Integer connectionTimeout = 10;

  @ConfigDef(
      required = true,
      label = "Socket Timeout",
      type = ConfigDef.Type.NUMBER,
      defaultValue = "50",
      description = "Set socket timeout (in seconds) for read and write operations. ",
      displayPosition = 4997,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public Integer socketTimeout = 50;

  @ConfigDef(
      required = true,
      label = "Retry Count",
      type = ConfigDef.Type.NUMBER,
      defaultValue = "3",
      description = "Sets the maximum number of retry attempts for failed " +
          "retry-able requests (ex: 5xx error).",
      displayPosition = 4999,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public Integer retryCount = 3;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Proxy",
      description = "Whether or not to connect to AWS through a proxy",
      defaultValue = "false",
      displayPosition = 5000,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public boolean useProxy;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Proxy Host",
      description = "Optional proxy host the client will connect through",
      displayPosition = 5010,
      dependsOn = "useProxy",
      triggeredByValue = "true",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public String proxyHost;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      label = "Proxy Port",
      description = "Optional proxy port the client will connect through",
      displayPosition = 5020,
      dependsOn = "useProxy",
      triggeredByValue = "true",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public int proxyPort;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Proxy User",
      description = "Optional proxy user name to use if connecting through a proxy",
      displayPosition = 5030,
      dependsOn = "useProxy",
      triggeredByValue = "true",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public CredentialValue proxyUser;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Proxy Password",
      description = "Optional proxy password to use when connecting through a proxy",
      displayPosition = 5040,
      dependsOn = "useProxy",
      triggeredByValue = "true",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public CredentialValue proxyPassword;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Proxy Domain",
      description = "Optional domain name for the proxy server",
      displayPosition = 5050,
      dependsOn = "useProxy",
      triggeredByValue = "true",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public String proxyDomain;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Proxy Workstation",
      description = "Optional workstation for the proxy server",
      displayPosition = 5060,
      dependsOn = "useProxy",
      triggeredByValue = "true",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public String proxyWorkstation;
}
