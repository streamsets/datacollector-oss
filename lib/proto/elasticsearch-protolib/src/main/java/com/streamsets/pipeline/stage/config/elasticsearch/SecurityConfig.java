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
import com.streamsets.pipeline.api.credential.CredentialValue;

public class SecurityConfig {

  public static final String CONF_PREFIX = "conf.securityConfig.";

  // Display positition here starts where ElasticsearchConfig stops. This is because this config is also available
  // on error stage where there is only one tab an hence all configs are sequential.
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "username:password",
      label = "Security Username/Password",
      dependsOn = "useSecurity^",
      triggeredByValue = "true",
      displayPosition = 41,
      group = "SECURITY"
  )
  public CredentialValue securityUser = () -> "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "SSL TrustStore Path",
      description = "",
      dependsOn = "useSecurity^",
      triggeredByValue = "true",
      displayPosition = 42,
      group = "SECURITY"
  )
  public String sslTrustStorePath;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "SSL TrustStore Password",
      dependsOn = "useSecurity^",
      triggeredByValue = "true",
      displayPosition = 43,
      group = "SECURITY"
  )
  public CredentialValue sslTrustStorePassword = () -> "";
}
