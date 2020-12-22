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
package com.streamsets.pipeline.stage.connection.elasticsearch;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import com.streamsets.pipeline.stage.lib.aws.AwsRegionChooserValues;

public class SecurityConfig {

  public static final String ENDPOINT_CONFIG_NAME = "endpoint";
  public static final String SECURITY_USER_CONFIG_NAME = "securityUser";
  public static final String SECURITY_PASSWORD_CONFIG_NAME = "securityPassword";
  public static final String ACCESS_KEY_ID_CONFIG_NAME = "awsAccessKeyId";
  public static final String SSL_TRUSTSTORE_PATH_CONFIG_NAME = "sslTrustStorePath";
  public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG_NAME = "sslTrustStorePassword";

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
      group = "#0"
  )
  @ValueChooserModel(SecurityModeChooserValues.class)
  public SecurityMode securityMode = SecurityMode.BASIC;

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
      group = "#0"
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
      group = "#0"
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
      group = "#0"
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
      group = "#0"
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
      group = "#0"
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
      group = "#0"
  )
  public CredentialValue securityPassword = () -> "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      label = "Enable SSL",
      defaultValue = "false",
      description = "Enables SSL usage",
      dependencies = {
          @Dependency(configName = "useSecurity^", triggeredByValues = "true")
      },
      displayPosition = 44,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public boolean enableSSL = false;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "SSL TrustStore Path",
      description = "",
      dependencies = {
          @Dependency(configName = "useSecurity^", triggeredByValues = "true"),
          @Dependency(configName = "enableSSL", triggeredByValues = "true")
      },
      triggeredByValue = "true",
      displayPosition = 45,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public String sslTrustStorePath;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "SSL TrustStore Password",
      dependencies = {
          @Dependency(configName = "useSecurity^", triggeredByValues = "true"),
          @Dependency(configName = "enableSSL", triggeredByValues = "true")
      },
      triggeredByValue = "true",
      displayPosition = 46,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public CredentialValue sslTrustStorePassword = () -> "";
}
