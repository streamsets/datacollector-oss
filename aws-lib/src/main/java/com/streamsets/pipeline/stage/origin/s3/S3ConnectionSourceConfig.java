/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.s3;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.common.s3.S3ConnectionBaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class S3ConnectionSourceConfig extends S3ConnectionBaseConfig {

  private final static Logger LOG = LoggerFactory.getLogger(S3ConnectionSourceConfig.class);

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Bucket",
    description = "DNS compatible bucket name.",
    displayPosition = 20,
    group = "#0"
  )
  public String bucket;

  public void init(
    Stage.Context context,
    String configPrefix,
    boolean prefixHasWildcard,
    List<Stage.ConfigIssue> issues,
    int maxErrorRetries
  ) {
    super.init(context, configPrefix, issues, maxErrorRetries);
    if (prefixHasWildcard && issues.isEmpty()) {
      validateConnection(context, configPrefix, issues);
    }
  }

  private void validateConnection(
      Stage.Context context,
      String configPrefix,
      List<Stage.ConfigIssue> issues
  ) {
    try {
      //check if the credentials are right by trying to list an object in the common prefix
      getS3Client().listObjects(new ListObjectsRequest(
          bucket,
          commonPrefix,
          null,
          delimiter,
          1
      ).withEncodingType("url"));
    } catch (AmazonS3Exception e) {
      LOG.debug(Errors.S3_SPOOLDIR_20.getMessage(), e.toString(), e);
      issues.add(
          context.createConfigIssue(
              Groups.S3.name(),
              configPrefix + S3ConnectionBaseConfig.AWS_CONFIG_PREFIX + "awsAccessKeyId",
              Errors.S3_SPOOLDIR_20,
              e.toString()
          )
      );
    }
  }
}
