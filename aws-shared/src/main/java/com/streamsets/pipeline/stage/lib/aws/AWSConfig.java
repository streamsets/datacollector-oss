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
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public class AWSConfig {

  @ConfigDef(displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Authentication Method",
      defaultValue = "WITH_CREDENTIALS",
      displayPosition = -120,
      group = "#0")
  @ValueChooserModel(AWSCredentialModeChooserValues.class)
  public AWSCredentialMode credentialMode = AWSCredentialMode.WITH_CREDENTIALS;

  @ConfigDef(displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Access Key ID",
      displayPosition = -110,
      group = "#0",
      dependsOn = "credentialMode",
      triggeredByValue = "WITH_CREDENTIALS")
  public CredentialValue awsAccessKeyId;

  @ConfigDef(displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Secret Access Key",
      displayPosition = -100,
      group = "#0",
      dependsOn = "credentialMode",
      triggeredByValue = "WITH_CREDENTIALS")
  public CredentialValue awsSecretAccessKey;

  @ConfigDef(displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Assume Role",
      description = "Temporarily assume a specified role to connect to Amazon",
      defaultValue = "false",
      displayPosition = -100,
      group = "#0",
      dependsOn = "credentialMode",
      triggeredByValue = {"WITH_IAM_ROLES", "WITH_CREDENTIALS"})
  public boolean isAssumeRole;

  @ConfigDef(displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Role ARN",
      description = "Amazon resource name of the role to assume",
      defaultValue = "arn:aws:iam::<account-id>:role/role-name",
      displayPosition = -100,
      group = "#0",
      dependsOn = "isAssumeRole",
      triggeredByValue = "true")
  public CredentialValue roleARN;

  @ConfigDef(displayMode = ConfigDef.DisplayMode.BASIC,
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Role Session Name",
      description = "Optional name for the session created by assuming a role. Overrides the default unique " +
          "identifier.",
      displayPosition = -100,
      group = "#0",
      dependsOn = "isAssumeRole",
      triggeredByValue = "true")
  public String roleSessionName = "";

  @ConfigDef(displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Session Timeout (secs)",
      description = "Maximum time for each session. Use a value in the following range: 3600 - 43200. " +
          "The session is refreshed if the pipeline continues to run.",
      displayPosition = -100,
      defaultValue = "3600",
      min = 3600,
      max = 43200,
      group = "#0",
      dependsOn = "isAssumeRole",
      triggeredByValue = "true")
  public int sessionDuration;

  @ConfigDef(displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Set Session Tags",
      description = "Specify session tags in the IAM policy to require that the currently logged in user who starts " +
          "the pipeline has access to assume the specified role",
      defaultValue = "true",
      displayPosition = -100,
      group = "#0",
      dependsOn = "isAssumeRole",
      triggeredByValue = "true")
  public boolean setSessionTags;

  /**
   * FakeS3 used for testing does not support chunked encoding
   * so it is exposed as a flag here, as the user should not
   * normally need to change this. This is a general AWS Client
   * configuration property and is not specific to S3.
   */
  public boolean disableChunkedEncoding = false; //NOSONAR
}
