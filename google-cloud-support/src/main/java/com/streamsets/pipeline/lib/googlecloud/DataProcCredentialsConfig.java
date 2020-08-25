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

package com.streamsets.pipeline.lib.googlecloud;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.stage.common.CredentialsProviderChooserValues;
import com.streamsets.pipeline.stage.common.CredentialsProviderType;

public class DataProcCredentialsConfig extends GoogleCloudCredentialsConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Project ID",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public String projectId = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Credentials Provider",
      defaultValue = "DEFAULT_PROVIDER",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  @ValueChooserModel(CredentialsProviderChooserValues.class)
  public CredentialsProviderType credentialsProvider;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Credentials File Path (JSON)",
      description = "Path to the credentials file.",
      dependsOn = "credentialsProvider",
      triggeredByValue = "JSON_PROVIDER",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public String path = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      mode = ConfigDef.Mode.JSON,
      label = "Credentials File Content (JSON)",
      description = "Content of the credentials file",
      dependsOn = "credentialsProvider",
      triggeredByValue = "JSON",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public CredentialValue credentialsFileContent;

  @Override
  public CredentialsProviderType getCredentialsProvider() {
    return credentialsProvider;
  }

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public String getProjectId() {
    return projectId;
  }

  @Override
  public CredentialValue getCredentialsFileContent() {
    return credentialsFileContent;
  }
}
