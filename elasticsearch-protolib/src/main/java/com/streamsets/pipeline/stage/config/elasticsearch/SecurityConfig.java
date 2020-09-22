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
package com.streamsets.pipeline.stage.config.elasticsearch;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import com.streamsets.pipeline.stage.lib.aws.AwsRegionChooserValues;

public class SecurityConfig {

  public static final String NAME = "securityConfig";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Mode",
      description = "Security mode to use for Elasticsearch authentication",
      dependsOn = "useSecurity^",
      triggeredByValue = "true",
      defaultValue = "BASIC",
      displayPosition = 37,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SECURITY"
  )
  @ValueChooserModel(SecurityModeChooserValues.class)
  public SecurityMode securityMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Region",
      dependencies = {
          @Dependency(configName = "useSecurity^", triggeredByValues = "true"),
          @Dependency(configName = "securityMode", triggeredByValues = "AWSSIGV4")
      },
      displayPosition = 38,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SECURITY"
  )
  @ValueChooserModel(AwsRegionChooserValues.class)
  public AwsRegion awsRegion;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Endpoint",
      description = "",
      defaultValue = "",
      displayPosition = 39,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "awsRegion",
      triggeredByValue = "OTHER",
      group = "SECURITY"
  )
  public String endpoint;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "",
      label = "Access Key ID",
      dependencies = {
          @Dependency(configName = "useSecurity^", triggeredByValues = "true"),
          @Dependency(configName = "securityMode", triggeredByValues = "AWSSIGV4")
      },
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SECURITY"
  )
  public CredentialValue awsAccessKeyId = () -> "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "",
      label = "Secret Access Key",
      dependencies = {
          @Dependency(configName = "useSecurity^", triggeredByValues = "true"),
          @Dependency(configName = "securityMode", triggeredByValues = "AWSSIGV4")
      },
      displayPosition = 41,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SECURITY"
  )
  public CredentialValue awsSecretAccessKey = () -> "";


  // Display positition here starts where ElasticsearchConfig stops. This is because this config is also available
  // on error stage where there is only one tab an hence all configs are sequential.
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "User Name",
      description = "Elasticsearch user name",
      dependencies = {
          @Dependency(configName = "useSecurity^", triggeredByValues = "true"),
          @Dependency(configName = "securityMode", triggeredByValues = "BASIC")
      },
      displayPosition = 42,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SECURITY"
  )
  public CredentialValue securityUser = () -> "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      description = "Elasticsearch password",
      dependencies = {
          @Dependency(configName = "useSecurity^", triggeredByValues = "true"),
          @Dependency(configName = "securityMode", triggeredByValues = "BASIC")
      },
      displayPosition = 43,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SECURITY"
  )
  public CredentialValue securityPassword = () -> "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "SSL TrustStore Path",
      description = "",
      dependsOn = "useSecurity^",
      triggeredByValue = "true",
      displayPosition = 44,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SECURITY"
  )
  public String sslTrustStorePath;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "SSL TrustStore Password",
      dependsOn = "useSecurity^",
      triggeredByValue = "true",
      displayPosition = 45,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SECURITY"
  )
  public CredentialValue sslTrustStorePassword = () -> "";
}
