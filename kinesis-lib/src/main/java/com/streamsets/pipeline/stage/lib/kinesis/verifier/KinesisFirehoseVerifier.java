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
package com.streamsets.pipeline.stage.lib.kinesis.verifier;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.ResourceNotFoundException;
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
import com.streamsets.pipeline.stage.destination.kinesis.Groups;
import com.streamsets.pipeline.stage.lib.aws.AWSKinesisUtil;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import com.streamsets.pipeline.stage.lib.kinesis.AwsKinesisConnectionGroups;
import com.streamsets.pipeline.stage.lib.kinesis.AwsKinesisFirehoseConnection;
import com.streamsets.pipeline.stage.lib.kinesis.Errors;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;

import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.REGION_PATTERN;

@StageDef(
    version = 1,
    label = "Amazon Kinesis Firehose Connection Verifier",
    description = "Verifies connections for Amazon Kinesis Firehose",
    upgraderDef = "upgrader/AwsKinesisConnectionVerifier.yaml",
    onlineHelpRefUrl = ""
)
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(AwsKinesisConnectionGroups.class)
@ConnectionVerifierDef(
    verifierType = AwsKinesisFirehoseConnection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection"
)
public class KinesisFirehoseVerifier extends ConnectionVerifier {
  // Important: if changing this, its length + the UUID (36) cannot be longer than 63 characters!
  private static final String STREAM_EXIST_PREFIX = "streamsets-kinesis-veri-";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = AwsKinesisFirehoseConnection.TYPE,
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
  public AwsKinesisFirehoseConnection connection;

  @Override
  protected List<ConfigIssue> initConnection() {
    List<ConfigIssue> issues = new ArrayList<>();
    Regions region = Regions.DEFAULT_REGION;
    try {
      AmazonKinesisFirehoseClientBuilder builder = AmazonKinesisFirehoseClientBuilder.standard();

      if (connection.region == AwsRegion.OTHER) {
        Matcher matcher = REGION_PATTERN.matcher(connection.endpoint);
        if (matcher.find()) {
          builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(connection.endpoint, null));
        } else {
          issues.add(getContext().createConfigIssue(
              com.streamsets.pipeline.stage.destination.kinesis.Groups.KINESIS.name(),
              "connection",
              Errors.KINESIS_19
          ));
        }
      } else {
        region = Regions.fromName(connection.region.getId().toLowerCase());
        builder.withRegion(region);
      }

      AmazonKinesisFirehose firehoseClient = builder
          .withCredentials(AWSKinesisUtil.getCredentialsProvider(connection.awsConfig, getContext(), region))
          .build();

      firehoseClient.describeDeliveryStream(new DescribeDeliveryStreamRequest()
          .withDeliveryStreamName(STREAM_EXIST_PREFIX + UUID.randomUUID().toString()));
    } catch (ResourceNotFoundException ex) {
      // this is expected as stream shouldn't exist
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(Groups.KINESIS.name(),
          "connection",
          Errors.KINESIS_22,
          ex.toString()
      ));
    }
    return issues;
  }
}
