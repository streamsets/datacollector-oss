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

package com.streamsets.pipeline.stage.pubsub.lib.connection;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.ListSubscriptionsRequest;
import com.google.pubsub.v1.Subscription;
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
import com.streamsets.pipeline.stage.GooglePubSubConnection;
import com.streamsets.pipeline.stage.common.CredentialsProviderType;
import com.streamsets.pipeline.stage.common.GoogleConnectionGroups;
import com.streamsets.pipeline.stage.pubsub.lib.Errors;
import com.streamsets.pipeline.stage.pubsub.lib.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.streamsets.pipeline.lib.googlecloud.GoogleCloudCredentialsConfig.CONF_CREDENTIALS_CREDENTIALS_PROVIDER;

@StageDef(version = 1,
    label = "Google Pub Sub Connection Verifier",
    description = "Verifies connections for Google Pub Sub",
    upgraderDef = "upgrader/PubSubConnectionVerifier.yaml",
    onlineHelpRefUrl = "")
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(GoogleConnectionGroups.class)
@ConnectionVerifierDef(verifierType = GooglePubSubConnection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection")
public class PubSubConnectionVerifier extends ConnectionVerifier {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubConnectionVerifier.class);

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = GooglePubSubConnection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection")
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String connectionSelection = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

  @ConfigDefBean(dependencies = {
      @Dependency(configName = "connectionSelection",
          triggeredByValues = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL)
  })
  public GooglePubSubConnection connection;

  @Override
  protected List<ConfigIssue> initConnection() {
    List<ConfigIssue> issues = new ArrayList<>();

    GoogleCloudCredentialsConfig credentialsConfig = new PubSubVerifierCredentialsConfig();

    credentialsConfig.getCredentialsProvider(getContext(), issues).ifPresent(provider -> {
      try {
        TopicAdminSettings.newBuilder().setCredentialsProvider(provider).build();
      } catch (IOException e) {
        LOG.error(Errors.PUBSUB_04.getMessage(), e.toString(), e);
        issues.add(getContext().createConfigIssue(Groups.CREDENTIALS.name(),
            CONF_CREDENTIALS_CREDENTIALS_PROVIDER,
            Errors.PUBSUB_04,
            e.toString()
        ));
      }

      if (issues.isEmpty()) {
        issues.addAll(testAdminSubscriptionSettings(provider));
      }
    });

    return issues;
  }

  private List<ConfigIssue> testAdminSubscriptionSettings(CredentialsProvider provider) {
    List<ConfigIssue> issues = new ArrayList<>();
    SubscriptionAdminSettings subAdminSettings = null;

    try {
      subAdminSettings = SubscriptionAdminSettings.newBuilder().setCredentialsProvider(provider).build();
    } catch (IOException e) {
      LOG.error(Errors.PUBSUB_04.getMessage(), e.toString(), e);
      issues.add(getContext().createConfigIssue(Groups.CREDENTIALS.name(),
          CONF_CREDENTIALS_CREDENTIALS_PROVIDER,
          Errors.PUBSUB_04,
          e.toString()
      ));
    }

    if (issues.isEmpty()) {
      issues.addAll(testAdminListSubscription(subAdminSettings));
    }

    return issues;
  }

  private List<ConfigIssue> testAdminListSubscription(SubscriptionAdminSettings subAdminSettings) {
    List<ConfigIssue> issues = new ArrayList<>();

    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(subAdminSettings)) {
      ListSubscriptionsRequest listSubscriptionsRequest = ListSubscriptionsRequest.newBuilder().setProject("projects/" +
          connection.getProjectId()).build();
      SubscriptionAdminClient.ListSubscriptionsPagedResponse response = subscriptionAdminClient.listSubscriptions(
          listSubscriptionsRequest);
      Iterable<Subscription> subscriptions = response.iterateAll();
      for (Subscription subscription : subscriptions) {
        LOG.info("Subscription '{}' exists for topic '{}'", subscription.getName(), subscription.getTopic());
      }
    } catch (IOException e) {
      LOG.error(Errors.PUBSUB_04.getMessage(), e.toString(), e);
      issues.add(getContext().createConfigIssue(Groups.CREDENTIALS.name(),
          CONF_CREDENTIALS_CREDENTIALS_PROVIDER,
          Errors.PUBSUB_04,
          e.toString()
      ));
    }

    return issues;
  }

  private class PubSubVerifierCredentialsConfig extends GoogleCloudCredentialsConfig {
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
