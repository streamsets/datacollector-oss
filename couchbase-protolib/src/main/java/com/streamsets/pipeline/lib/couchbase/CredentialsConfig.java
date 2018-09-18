/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.lib.couchbase;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;

public class CredentialsConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "USER",
      label = "Authentication Mode",
      description = "Use bucket authentication for Couchbase 4. Use user authentication for Couchbase 5 or later",
      displayPosition = 10,
      group = "CREDENTIALS"
  )
  @ValueChooserModel(AuthenticationTypeChooserValues.class)
  public AuthenticationType version = AuthenticationType.USER;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Bucket Password",
      displayPosition = 20,
      description = "Bucket password",
      dependsOn = "version",
      triggeredByValue = "BUCKET",
      group = "CREDENTIALS"
  )
  public CredentialValue bucketPassword;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "User Name",
      displayPosition = 30,
      description = "Couchbase user name",
      dependsOn = "version",
      triggeredByValue = "USER",
      group = "CREDENTIALS"
  )
  public CredentialValue userName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      displayPosition = 40,
      description = "Couchbase user password",
      dependsOn = "version",
      triggeredByValue = "USER",
      group = "CREDENTIALS"
  )
  public CredentialValue userPassword;

}
