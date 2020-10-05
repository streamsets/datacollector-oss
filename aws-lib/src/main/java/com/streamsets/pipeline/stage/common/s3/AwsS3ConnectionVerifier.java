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
package com.streamsets.pipeline.stage.common.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
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
import com.streamsets.pipeline.stage.origin.s3.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@StageDef(
    version = 1,
    label = "Amazon S3 Connection Verifier",
    description = "Verifies connections for Amazon S3",
    upgraderDef = "upgrader/AwsS3ConnectionVerifier.yaml",
    onlineHelpRefUrl = ""
)
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(AwsS3ConnectionGroups.class)
@ConnectionVerifierDef(
    verifierType = AwsS3Connection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection"
)
public class AwsS3ConnectionVerifier extends ConnectionVerifier {
  private static final Logger LOG = LoggerFactory.getLogger(AwsS3ConnectionVerifier.class);

  // Important: if changing this, its length + the UUID (36) cannot be longer than 63 characters!
  private static final String BUCKET_EXIST_PREFIX = "streamsets-s3-conn-veri-";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = AwsS3Connection.TYPE,
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
  public AwsS3Connection connection;

  private AmazonS3 s3Client;

  @Override
  protected List<ConfigIssue> initConnection() {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    try {
      s3Client = S3ConnectionCreator.createS3Client(connection, getContext(), "connection", issues, -1, false);
      if (issues.isEmpty()) {
        checkConnection(s3Client);
      }
    } catch (Exception e) {
      LOG.debug(Errors.S3_SPOOLDIR_20.getMessage(), e.getMessage(), e);
      issues.add(getContext().createConfigIssue("S3", "connection", Errors.S3_SPOOLDIR_20, e.toString()));
    }
    return issues;
  }

  static void checkConnection(AmazonS3 s3Client) throws InterruptedException {
    String bucket = BUCKET_EXIST_PREFIX + UUID.randomUUID().toString();
    // Not exactly sure why (might be some weird JVM DNS caching issue?) but occasionally we'll get an
    // UnknownHostException on this, even though everything is fine.  In that case, let's try it again a few
    // times with a delay.  Otherwise (or if that still fails), we'll give up and throw it.
    for (int attempt = 1; attempt <= 5; attempt++) {
      try {
        // We don't actually care if the bucket exists, we're only interested in if this will throw an Exception
        s3Client.doesBucketExistV2(bucket);
        break;
      } catch (SdkClientException e) {
        if (e.getCause() != null && e.getCause() instanceof UnknownHostException) {
          if (attempt >= 5) {
            LOG.debug("UnknownHostException connecting to S3 - exceeded {} of 5 attempts, giving up", attempt);
            throw e;
          }
          int sleepTime = 3 * attempt;
          LOG.debug("UnknownHostException connecting to S3 - attempt {} of 5, will retry in {} seconds",
              attempt, sleepTime);
          Thread.sleep(sleepTime * 1000);
        } else {
          throw e;
        }
      }
    }
  }

  @Override
  protected void destroyConnection() {
    S3ConnectionCreator.destroyS3Client(s3Client);
  }
}
