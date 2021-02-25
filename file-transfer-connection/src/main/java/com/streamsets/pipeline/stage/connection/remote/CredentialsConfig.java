/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.pipeline.stage.connection.remote;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;

public class CredentialsConfig {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NONE",
      label = "Authentication",
      description = "The authentication method to use to login to remote server",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  @ValueChooserModel(AuthenticationChooserValues.class)
  public Authentication auth = Authentication.NONE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      description = "Username to use to login to the remote server",
      displayPosition = 15,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependsOn = "auth",
      triggeredByValue = {"PASSWORD", "PRIVATE_KEY"}
  )
  public CredentialValue username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      description = "Password to use to login to the remote server. If private key is specified, that is used.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependsOn = "auth",
      triggeredByValue = {"PASSWORD"}
  )
  public CredentialValue password;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Strict Host Checking",
      description = "If enabled, will only connect to the host if the host is in the known hosts file.",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependencies = {
          @Dependency(configName = "^",
              triggeredByValues = "SFTP")
      }
  )
  public boolean strictHostChecking;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Known Hosts file",
      description = "Full path to the file that lists the host keys of all known hosts." +
          "This must be specified if the strict host checking is enabled.",
      group = "#0",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "strictHostChecking",
      triggeredByValue = "true"
  )
  public String knownHosts;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Use Client Certificate for FTPS",
      description = "Enable this if the FTPS Server requires mutual authentication. The client will need to provide " +
          "a keystore file containing the client certificate.",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependsOn = "protocol^",
      triggeredByValue = "FTPS"
  )
  public boolean useFTPSClientCert;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "ALLOW_ALL",
      label = "FTPS Truststore Provider",
      description = "Providing a Truststore allows the client to verify the FTPS Server's certificate. " +
          "\"Allow All\" will allow any certificate, skipping validation. " +
          "\"File\" will allow providing a truststore file containing the certificate. " +
          "\"Remote Truststore\" allows providing a list of trusted certificates to build the truststore" +
          "\"JVM Default\" will use the JVM's default truststore.",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependsOn = "protocol^",
      triggeredByValue = "FTPS"
  )
  @ValueChooserModel(FTPSTrustStoreChooserValues.class)
  public FTPSTrustStore ftpsTrustStoreProvider = FTPSTrustStore.ALLOW_ALL;
}
