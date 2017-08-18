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
package com.streamsets.pipeline.stage.common.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.common.InterfaceAudience;
import com.streamsets.pipeline.common.InterfaceStability;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.AWSRegionChooserValues;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import com.streamsets.pipeline.stage.lib.aws.ProxyConfig;
import com.streamsets.pipeline.stage.origin.s3.Errors;
import com.streamsets.pipeline.stage.origin.s3.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public abstract class S3ConnectionBaseConfig {

  public static final String AWS_CONFIG_PREFIX = "awsConfig.";
  private final static Logger LOG = LoggerFactory.getLogger(S3ConnectionBaseConfig.class);

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
    try {
      createConnection(context, configPrefix, proxyConfig, issues, maxErrorRetries);
    } catch (StageException ex) {
      LOG.debug(Errors.S3_SPOOLDIR_20.getMessage(), ex.toString(), ex);
      issues.add(
          context.createConfigIssue(
              Groups.S3.name(),
              configPrefix + S3ConnectionBaseConfig.AWS_CONFIG_PREFIX + "awsAccessKeyId",
              Errors.S3_SPOOLDIR_20,
              ex.toString()
          )
      );
    }
  }

  public void destroy() {
    if(s3Client != null) {
      s3Client.shutdown();
    }
  }

  public AmazonS3 getS3Client() {
    return s3Client;
  }

  private AmazonS3 s3Client;

  private void createConnection(
      Stage.Context context,
      String configPrefix,
      ProxyConfig proxyConfig,
      List<Stage.ConfigIssue> issues,
      int maxErrorRetries
  ) throws StageException {
    AWSCredentialsProvider credentials = AWSUtil.getCredentialsProvider(awsConfig);
    ClientConfiguration clientConfig = AWSUtil.getClientConfiguration(proxyConfig);

    if (maxErrorRetries >= 0) {
      clientConfig.setMaxErrorRetry(maxErrorRetries);
    }

    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder
        .standard()
        .withCredentials(credentials)
        .withClientConfiguration(clientConfig)
        .withChunkedEncodingDisabled(awsConfig.disableChunkedEncoding)
        .withPathStyleAccessEnabled(true);

    if (region == AWSRegions.OTHER) {
      if (endpoint == null || endpoint.isEmpty()) {
        issues.add(context.createConfigIssue(Groups.S3.name(), configPrefix + "endpoint", Errors.S3_SPOOLDIR_10));
        return;
      }
      builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, null));
    } else {
      builder.withRegion(region.getLabel());
    }
    s3Client = builder.build();
  }
}
