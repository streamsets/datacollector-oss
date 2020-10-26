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

package com.streamsets.pipeline.lib.salesforce.connection;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.salesforce.connection.mutualauth.MutualAuthConfigBean;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
@ConnectionDef(
    label = "Salesforce",
    type = SalesforceConnection.TYPE,
    description = "Connects to Salesforce and Salesforce Einstein Analytics",
    version = 1,
    upgraderDef = "upgrader/SalesforceConnectionUpgrader.yaml",
    supportedEngines = { ConnectionEngine.COLLECTOR }
)
@ConfigGroups(SalesforceConnectionGroups.class)
public class SalesforceConnection {

  public static final String TYPE = "STREAMSETS_SALESFORCE";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      displayPosition = 10,
      group = "FORCE"
  )
  public CredentialValue username;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      displayPosition = 20,
      group = "FORCE"
  )
  public CredentialValue password;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      defaultValue = "login.salesforce.com",
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Auth Endpoint",
      displayPosition = 30,
      group = "FORCE"
  )
  public String authEndpoint;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      defaultValue = "43.0",
      required = true,
      type = ConfigDef.Type.STRING,
      label = "API Version",
      displayPosition = 40,
      group = "FORCE"
  )
  public String apiVersion;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      label = "Use Proxy",
      type = ConfigDef.Type.BOOLEAN,
      description = "Connect to Salesforce via a proxy server.",
      defaultValue = "false",
      displayPosition = 10,
      group = "ADVANCED"
  )
  public boolean useProxy = false;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      label = "Proxy Hostname",
      type = ConfigDef.Type.STRING,
      description = "Proxy Server Hostname",
      defaultValue = "",
      displayPosition = 20,
      group = "ADVANCED",
      dependsOn = "useProxy",
      triggeredByValue = "true"
  )
  public String proxyHostname = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      label = "Proxy Port",
      type = ConfigDef.Type.NUMBER,
      description = "Proxy Server Hostname",
      defaultValue = "",
      displayPosition = 30,
      group = "ADVANCED",
      dependsOn = "useProxy",
      triggeredByValue = "true"
  )
  public int proxyPort = 0;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      label = "Proxy Requires Credentials",
      type = ConfigDef.Type.BOOLEAN,
      description = "Enable if you need to supply a username/password to connect via the proxy server.",
      defaultValue = "false",
      displayPosition = 40,
      group = "ADVANCED",
      dependsOn = "useProxy",
      triggeredByValue = "true"
  )
  public boolean useProxyCredentials = false;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      label = "Proxy Realm",
      type = ConfigDef.Type.CREDENTIAL,
      description = "Authenticaton realm for the proxy server.",
      displayPosition = 50,
      group = "ADVANCED",
      dependencies = {
          @Dependency(configName = "useProxy", triggeredByValues = "true"),
          @Dependency(configName = "useProxyCredentials", triggeredByValues = "true")
      }
  )
  public CredentialValue proxyRealm;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      label = "Proxy Username",
      type = ConfigDef.Type.CREDENTIAL,
      description = "Username for the proxy server.",
      displayPosition = 60,
      group = "ADVANCED",
      dependencies = {
          @Dependency(configName = "useProxy", triggeredByValues = "true"),
          @Dependency(configName = "useProxyCredentials", triggeredByValues = "true")
      }
  )
  public CredentialValue proxyUsername;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      label = "Proxy Password",
      type = ConfigDef.Type.CREDENTIAL,
      description = "Password for the proxy server.",
      displayPosition = 70,
      group = "ADVANCED",
      dependencies = {
          @Dependency(configName = "useProxy", triggeredByValues = "true"),
          @Dependency(configName = "useProxyCredentials", triggeredByValues = "true")
      }
  )
  public CredentialValue proxyPassword;

  @ConfigDefBean(groups = "ADVANCED")
  public MutualAuthConfigBean mutualAuth = new MutualAuthConfigBean();
}
