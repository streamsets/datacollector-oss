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

package com.streamsets.pipeline.stage.cloudstorage.lib.connection;

import com.google.cloud.storage.StorageOptions;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionVerifier;
import com.streamsets.pipeline.api.ConnectionVerifierDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.googlecloud.GoogleCloudCredentialsConfig;
import com.streamsets.pipeline.stage.GoogleCloudStorageConnection;
import com.streamsets.pipeline.stage.cloudstorage.lib.Errors;
import com.streamsets.pipeline.stage.cloudstorage.origin.Groups;
import com.streamsets.pipeline.stage.common.CredentialsProviderType;
import com.streamsets.pipeline.stage.common.GoogleConnectionGroups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


@StageDef(version = 1,
    label = "Google Cloud Storage Connection Verifier",
    description = "Verifies connections for Google Cloud Storage",
    upgraderDef = "upgrader/CloudStorageConnectionVerifier.yaml",
    onlineHelpRefUrl = "")
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(GoogleConnectionGroups.class)
@ConnectionVerifierDef(verifierType = GoogleCloudStorageConnection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection")
public class CloudStorageConnectionVerifier extends ConnectionVerifier {

  private static final Logger LOG = LoggerFactory.getLogger(CloudStorageConnectionVerifier.class);

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = GoogleCloudStorageConnection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection")
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String connectionSelection = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

  @ConfigDefBean(dependencies = {
      @Dependency(configName = "connectionSelection",
          triggeredByValues = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL)
  })
  public GoogleCloudStorageConnection connection;

  @Override
  protected List<ConfigIssue> initConnection() {
    List<ConfigIssue> issues = new ArrayList<>();

    GoogleCloudCredentialsConfig credentialsConfig = new CloudStorageVerifierCredentialsConfig();

    credentialsConfig.getCredentialsProvider(getContext(), issues).ifPresent(provider -> {
      try {
        StorageOptions.newBuilder().setCredentials(provider.getCredentials()).build().getService();
      } catch (IOException | NullPointerException e) {
        LOG.error("Error when initializing storage. Reason : {}", e.toString());
        issues.add(getContext().createConfigIssue(Groups.CREDENTIALS.name(),
            "gcsOriginConfig.credentials.credentialsProvider",
            Errors.GCS_01,
            e
        ));
      }
    });

    return issues;
  }

  private class CloudStorageVerifierCredentialsConfig extends GoogleCloudCredentialsConfig {
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
      return connection.credentialsFileContent;
    }
  }
}