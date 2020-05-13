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

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConnectionVerifier;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.origin.s3.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class AwsS3ConnectionVerifier extends ConnectionVerifier {
  private final static Logger LOG = LoggerFactory.getLogger(AwsS3ConnectionVerifier.class);

  // Important: if changing this, its length + the UUID (36) cannot be longer than 63 characters!
  private static final String BUCKET_EXIST_PREFIX = "streamsets-s3-conn-veri-";

  @ConfigDefBean(groups = "S3")
  public AwsS3Connection connection;

  @Override
  protected List<ConfigIssue> initConnection() {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    try {
      connection.initConnection(getContext(), "connection", issues, -1, false);
      if (!issues.isEmpty()) {
        // We don't actually care if the bucket exists or not, we're only interested in if this will throw an Exception
        connection.getS3Client().doesBucketExistV2(BUCKET_EXIST_PREFIX + UUID.randomUUID().toString());
      }
    } catch (Exception e) {
      LOG.debug(Errors.S3_SPOOLDIR_20.getMessage(), e.getMessage(), e);
      issues.add(getContext().createConfigIssue("S3", "connection", Errors.S3_SPOOLDIR_20, e.toString()));
    }
    return issues;
  }

  @Override
  protected void destroyConnection() {
    connection.destroy();
  }
}
