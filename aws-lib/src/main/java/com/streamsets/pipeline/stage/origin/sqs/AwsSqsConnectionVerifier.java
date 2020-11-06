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

package com.streamsets.pipeline.stage.origin.sqs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionVerifier;
import com.streamsets.pipeline.api.ConnectionVerifierDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.common.sqs.AwsSqsConnection;
import com.streamsets.pipeline.stage.common.sqs.AwsSqsConnectionGroups;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@StageDef(
    version = 1,
    label = "Amazon SQS Connection Verifier",
    description = "Verifies connections for Amazon SQS",
    upgraderDef = "upgrader/AwsSqsConnectionVerifier.yaml",
    onlineHelpRefUrl = ""
)
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(AwsSqsConnectionGroups.class)
@ConnectionVerifierDef(
    verifierType = AwsSqsConnection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection"
)
public class AwsSqsConnectionVerifier extends ConnectionVerifier {

  private static final Logger LOG = LoggerFactory.getLogger(AwsSqsConnectionVerifier.class);
  
  private static final String SQS_CONNECTION_CONFIG_PREFIX = "connection";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = AwsSqsConnection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection"
  )
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String connectionSelection = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

  @ConfigDefBean(
      dependencies = {
          @Dependency(
              configName = "connectionSelection",
              triggeredByValues = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL
          )
      }
  )
  public AwsSqsConnection connection;

  @Override
  protected List<ConfigIssue> initConnection() {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    if (connection.region == AwsRegion.OTHER && (connection.endpoint == null || connection.endpoint.isEmpty())) {
      issues.add(getContext().createConfigIssue(
          Groups.SQS.name(),
          SQS_CONNECTION_CONFIG_PREFIX + "endpoint",
          Errors.SQS_01
      ));
    } else {
      ClientConfiguration clientConfiguration = null;
      AWSCredentialsProvider credentials = null;

      try {
        clientConfiguration = AWSUtil.getClientConfiguration(connection.proxyConfig);
      } catch (StageException e) {
        issues.add(getContext().createConfigIssue(Groups.SQS.name(),
            SQS_CONNECTION_CONFIG_PREFIX + "proxyConfig",
            Errors.SQS_10,
            e.getMessage(),
            e
        ));
      }

      try {
        Regions regions = Regions.DEFAULT_REGION;

        if (connection.region.equals(AwsRegion.OTHER)) {
          regions = Regions.fromName(connection.region.getId().toLowerCase());
        }

        credentials = AWSUtil.getCredentialsProvider(connection.awsConfig, getContext(), regions);
      } catch (StageException e) {
        issues.add(getContext().createConfigIssue(Groups.SQS.name(),
            SQS_CONNECTION_CONFIG_PREFIX + "awsConfig",
            Errors.SQS_11,
            e.getMessage(),
            e
        ));
      }

      if (issues.isEmpty()) {
        AmazonSQSClientBuilder builder = AmazonSQSClientBuilder.standard()
                                                               .withClientConfiguration(clientConfiguration)
                                                               .withCredentials(credentials);
        if (connection.region == AwsRegion.OTHER) {
          builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(connection.endpoint, null));
        } else {
          builder.withRegion(connection.region.getId());
        }

        try {
          AmazonSQS validationClient = builder.build();
          // We don't actually care if there are queues or not, we're only interested in if this will throw an Exception
          validationClient.listQueues();
        } catch (Exception e) {
          LOG.error(Utils.format(Errors.SQS_14.getMessage(), e.toString()), e);
          issues.add(getContext().createConfigIssue(Groups.SQS.name(),
              SQS_CONNECTION_CONFIG_PREFIX,
              Errors.SQS_14,
              e.getMessage(),
              e
          ));
        }
      }
    }

    return issues;
  }

}
