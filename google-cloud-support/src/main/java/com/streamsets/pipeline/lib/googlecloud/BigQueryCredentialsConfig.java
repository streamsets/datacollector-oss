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
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.stage.GoogleBigQueryConnection;
import com.streamsets.pipeline.stage.common.CredentialsProviderType;

public class BigQueryCredentialsConfig extends GoogleCloudCredentialsConfig {

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = GoogleBigQueryConnection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection",
      group = "#0",
      displayPosition = -500)
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String connectionSelection = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

  @ConfigDefBean(dependencies = {
      @Dependency(configName = "connectionSelection",
          triggeredByValues = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL)
  })
  public GoogleBigQueryConnection connection = new GoogleBigQueryConnection();

  @Override
  public CredentialsProviderType getCredentialsProvider() {
    return connection.getCredentialsProvider();
  }

  @Override
  public String getPath() {
    return connection.getPath();
  }

  @Override
  public String getProjectId() {
    return connection.getProjectId();
  }

  @Override
  public CredentialValue getCredentialsFileContent() {
    return connection.getCredentialsFileContent();
  }
}
