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

package com.streamsets.pipeline.stage.bigquery.lib.connection;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.common.annotations.VisibleForTesting;
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
import com.streamsets.pipeline.stage.GoogleBigQueryConnection;
import com.streamsets.pipeline.stage.bigquery.lib.BigQueryDelegate;
import com.streamsets.pipeline.stage.bigquery.lib.Errors;
import com.streamsets.pipeline.stage.bigquery.lib.Groups;
import com.streamsets.pipeline.stage.common.CredentialsProviderType;
import com.streamsets.pipeline.stage.common.GoogleConnectionGroups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@StageDef(version = 1,
    label = "Google BigQuery Connection Verifier",
    description = "Verifies connections for Google BigQuery",
    upgraderDef = "upgrader/BigQueryConnectionVerifier.yaml",
    onlineHelpRefUrl = "")
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(GoogleConnectionGroups.class)
@ConnectionVerifierDef(verifierType = GoogleBigQueryConnection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection")
public class BigQueryConnectionVerifier extends ConnectionVerifier {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryConnectionVerifier.class);

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = GoogleBigQueryConnection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection")
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String connectionSelection = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

  @ConfigDefBean(dependencies = {
      @Dependency(configName = "connectionSelection",
          triggeredByValues = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL)
  })
  public GoogleBigQueryConnection connection;

  @Override
  protected List<ConfigIssue> initConnection() {
    List<ConfigIssue> issues = new ArrayList<>();

    GoogleCloudCredentialsConfig credentialsConfig = new BigQueryVerifierCredentialsConfig();

    credentialsConfig.getCredentialsProvider(getContext(), issues).ifPresent(provider -> {
      if (issues.isEmpty()) {
        try {
          Optional.ofNullable(provider.getCredentials()).ifPresent(credentials -> new BigQueryDelegate(getBigQuery(
              credentials), false));
        } catch (IOException e) {
          LOG.error(Errors.BIGQUERY_05.getMessage(), e);
          issues.add(getContext().createConfigIssue(Groups.CREDENTIALS.name(),
              "conf.credentials.connection.credentialsProvider",
              Errors.BIGQUERY_05
          ));
        }
      }
    });
    return issues;
  }

  @VisibleForTesting
  BigQuery getBigQuery(Credentials credentials) {
    return BigQueryDelegate.getBigquery(credentials, connection.getProjectId());
  }

  private class BigQueryVerifierCredentialsConfig extends GoogleCloudCredentialsConfig {
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
