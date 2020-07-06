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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.common.InterfaceAudience;
import com.streamsets.pipeline.common.InterfaceStability;
import com.streamsets.pipeline.lib.aws.AwsRegion;
import com.streamsets.pipeline.lib.aws.AwsRegionChooserValues;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import com.streamsets.pipeline.stage.lib.aws.ProxyConfig;
import com.streamsets.pipeline.stage.origin.s3.Errors;
import com.streamsets.pipeline.stage.origin.s3.Groups;

import java.util.List;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
@ConnectionDef(
    label = "Amazon S3",
    type = AwsS3Connection.TYPE,
    description = "Connects to Amazon S3",
    version = 1,
    upgraderDef = "upgrader/AwsS3Connection.yaml",
    verifier = AwsS3ConnectionVerifier.class,
    supportedEngines = {ConnectionEngine.COLLECTOR, ConnectionEngine.TRANSFORMER}
)
@ConfigGroups(AwsS3ConnectionGroups.class)
public class AwsS3Connection {

  public static final String TYPE = "STREAMSETS_AWS_S3";

  @ConfigDefBean()
  public AWSConfig awsConfig;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "US_WEST_2",
      label = "Region",
      displayPosition = -95,
      group = "#0"
  )
  @ValueChooserModel(AwsRegionChooserValues.class)
  public AwsRegion region;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Endpoint",
      description = "",
      defaultValue = "",
      displayPosition = -90,
      dependsOn = "region",
      triggeredByValue = "OTHER",
      group = "#0"
  )
  public String endpoint;

  @ConfigDefBean(groups = "#1")
  public ProxyConfig proxyConfig;

  private AmazonS3 s3Client;

  public AmazonS3 getS3Client() {
    return s3Client;
  }

  public void destroy() {
    if(s3Client != null) {
      s3Client.shutdown();
    }
  }

  public void initConnection(
      Stage.Context context,
      String configPrefix,
      List<Stage.ConfigIssue> issues,
      int maxErrorRetries,
      boolean usePathAddressModel) {
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
        .withPathStyleAccessEnabled(usePathAddressModel);

    if (region == AwsRegion.OTHER) {
      if (endpoint == null || endpoint.isEmpty()) {
        issues.add(context.createConfigIssue(Groups.S3.name(), configPrefix + "endpoint", Errors.S3_SPOOLDIR_10));
        return;
      }
      builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, null));
    } else {
      builder.withRegion(region.getId());
    }
    s3Client = builder.build();

  }
}
