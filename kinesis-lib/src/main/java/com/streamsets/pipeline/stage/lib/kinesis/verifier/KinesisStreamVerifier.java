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
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.lib.aws.AWSKinesisUtil;
import com.streamsets.pipeline.stage.lib.kinesis.AwsKinesisStreamConnection;
import com.streamsets.pipeline.stage.lib.kinesis.AwsKinesisConnectionGroups;
import com.streamsets.pipeline.stage.lib.kinesis.Errors;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil;
import com.streamsets.pipeline.stage.origin.kinesis.Groups;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@StageDef(
    version = 1,
    label = "Amazon Kinesis Stream Connection Verifier",
    description = "Verifies connections for Amazon Kinesis DataStreams",
    upgraderDef = "upgrader/AwsKinesisConnectionVerifier.yaml",
    onlineHelpRefUrl = ""
)
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(AwsKinesisConnectionGroups.class)
@ConnectionVerifierDef(
    verifierType = AwsKinesisStreamConnection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection"
)
public class KinesisStreamVerifier extends ConnectionVerifier {
  private final static Logger LOG = LoggerFactory.getLogger(KinesisStreamVerifier.class);
  // Important: if changing this, its length + the UUID (36) cannot be longer than 63 characters!
  private static final String STREAM_EXIST_PREFIX = "streamsets-kinesis-veri-";
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = AwsKinesisStreamConnection.TYPE,
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
  public AwsKinesisStreamConnection connection;

  @Override
  protected List<ConfigIssue> initConnection() {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    try {
      KinesisUtil.getShardCount(
          AWSKinesisUtil.getClientConfiguration(connection.proxyConfig),
          connection,
          STREAM_EXIST_PREFIX + UUID.randomUUID().toString(),
          getContext()
      );
    } catch (ResourceNotFoundException ex) {
      // expected as stream shouldn't exist
    } catch (Exception ex) {
      LOG.error(Utils.format(Errors.KINESIS_12.getMessage(), ex.toString()), ex);
      issues.add(getContext().createConfigIssue(Groups.KINESIS.name(), "connection", Errors.KINESIS_12, ex.toString()));
    }
    return issues;
  }
}
