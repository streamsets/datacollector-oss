/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.common.InterfaceAudience;
import com.streamsets.pipeline.common.InterfaceStability;
import com.streamsets.pipeline.stage.lib.aws.AWSRegionChooserValues;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import com.streamsets.pipeline.stage.lib.aws.ProxyConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public class S3Config {

  private static final String AWS_CONFIG_PREFIX = "awsConfig.";
  private final static Logger LOG = LoggerFactory.getLogger(S3Config.class);

  @ConfigDefBean(groups = "S3")
  public AWSConfig awsConfig;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "US_WEST_2",
    label = "Region",
    displayPosition = 10,
    group = "#0"
  )
  @ValueChooserModel(AWSRegionChooserValues.class)
  public AWSRegions region;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Endpoint",
      description = "",
      defaultValue = "",
      displayPosition = 15,
      dependsOn = "region",
      triggeredByValue = "OTHER",
      group = "#0"
  )
  public String endpoint;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Bucket",
    description = "",
    displayPosition = 20,
    group = "#0"
  )
  public String bucket;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Common Prefix",
    description = "",
    displayPosition = 30,
    group = "#0"
  )
  public String commonPrefix;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Delimiter",
    description = "",
    defaultValue = "/",
    displayPosition = 40,
    group = "#0"
  )
  public String delimiter;

  /* Max Error retries >=0 are set in ClientConfig for S3Client, < 0 will use default (3)
   */
  public void init(
      Stage.Context context,
      String configPrefix,
      ProxyConfig proxyConfig,
      List<Stage.ConfigIssue> issues,
      int maxErrorRetries
  ) {
    commonPrefix = AWSUtil.normalizePrefix(commonPrefix, delimiter);
    validateConnection(context, configPrefix, proxyConfig, issues, maxErrorRetries);
  }

  public void destroy() {
    if(s3Client != null) {
      s3Client.shutdown();
    }
  }

  public AmazonS3Client getS3Client() {
    return s3Client;
  }

  private AmazonS3Client s3Client;

  private void validateConnection(
      Stage.Context context,
      String configPrefix,
      ProxyConfig proxyConfig,
      List<Stage.ConfigIssue> issues,
      int maxErrorRetries
  ) {
    AWSCredentialsProvider credentials = AWSUtil.getCredentialsProvider(awsConfig);
    ClientConfiguration clientConfig = AWSUtil.getClientConfiguration(proxyConfig);

    if (maxErrorRetries >= 0) {
      clientConfig.setMaxErrorRetry(maxErrorRetries);
    }

    s3Client = new AmazonS3Client(credentials, clientConfig);
    s3Client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
    if (region == AWSRegions.OTHER) {
      if (endpoint == null || endpoint.isEmpty()) {
        issues.add(context.createConfigIssue(Groups.S3.name(), configPrefix + "endpoint", Errors.S3_SPOOLDIR_10));
        return;
      }
      s3Client.setEndpoint(endpoint);
    } else {
      s3Client.setRegion(RegionUtils.getRegion(region.getLabel()));
    }
    try {
      //check if the credentials are right by trying to list an object in the common prefix
      s3Client.listObjects(new ListObjectsRequest(bucket, commonPrefix, null, delimiter, 1).withEncodingType("url"));
    } catch (AmazonS3Exception e) {
      LOG.debug(Errors.S3_SPOOLDIR_20.getMessage(), e.toString(), e);
      issues.add(
          context.createConfigIssue(
              Groups.S3.name(),
              configPrefix + AWS_CONFIG_PREFIX + "awsAccessKeyId",
              Errors.S3_SPOOLDIR_20,
              e.toString()
          )
      );
    }
  }
}
