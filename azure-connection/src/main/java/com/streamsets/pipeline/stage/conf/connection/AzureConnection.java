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
package com.streamsets.pipeline.stage.conf.connection;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public abstract class AzureConnection {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "example.dfs.core.windows.net",
      label = "Account FQDN",
      description = "The fully qualified domain name of the Data Lake Storage account",
      displayPosition = 1,
      group = "#0"
  )
  public CredentialValue accountFQDN;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "example-blob-container",
      label = "Storage Container / File System",
      description = "Name of the storage container or file system in the storage account",
      displayPosition = 2,
      group = "#0"
  )
  public CredentialValue storageContainer;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "CLIENT",
      label = "Authentication Method",
      description = "Method used to authenticate connections to Azure",
      displayPosition = 4,
      group = "#0"
  )
  @ValueChooserModel(OAuthProviderTypeValues.class)
  public OAuthProviderType authMethod;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Application ID",
      description = "Azure application id",
      displayPosition = 5,
      group = "#0",
      dependsOn = "authMethod",
      triggeredByValue = "CLIENT"
  )
  public CredentialValue clientId;

  @ConfigDef(required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "",
      label = "Tenant ID",
      description = "Azure tenant ID",
      displayPosition = 6,
      group = "#0",
      dependsOn = "authMethod",
      triggeredByValue = "CLIENT"
  )
  public CredentialValue tenantId;

  @ConfigDef(required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "",
      label = "Application Key",
      description = "Azure application key",
      displayPosition = 7,
      group = "#0",
      dependsOn = "authMethod",
      triggeredByValue = "CLIENT"
  )
  public CredentialValue clientKey;

  @ConfigDef(required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "",
      label = "Account Shared Key",
      description = "Azure storage account shared key",
      displayPosition = 7,
      group = "#0",
      dependsOn = "authMethod",
      triggeredByValue = "SHARED_KEY"
  )
  public CredentialValue accountKey;
}
