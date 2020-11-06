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
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import com.streamsets.pipeline.stage.origin.s3.Errors;
import com.streamsets.pipeline.stage.origin.s3.Groups;

import java.util.List;

public class S3ConnectionCreator {

  private S3ConnectionCreator() {
    //Private empty constructor
  }

  public static void destroyS3Client(AmazonS3 s3Client) {
    if (s3Client != null) {
      s3Client.shutdown();
    }
  }

  /**
   * Creates an Amazon S3 client with given connection
   *
   * @param connection The connection to use in the S3 client
   * @param context The stage context
   * @param configPrefix The prefix for the configuration bean
   * @param issues List of issues
   * @param maxErrorRetries Maximum number of retries on error
   * @param usePathAddressModel Whether to use the path address model
   * @return The Amazon S3 client
   */
  public static AmazonS3 createS3Client(
      AwsS3Connection connection,
      Stage.Context context,
      String configPrefix,
      List<Stage.ConfigIssue> issues,
      int maxErrorRetries,
      boolean usePathAddressModel
  ) {
    Regions regions = Regions.DEFAULT_REGION;

    if (connection.useRegion && !connection.region.equals(AwsRegion.OTHER)){
      regions = Regions.fromName(connection.region.getId().toLowerCase());
    }

    AWSCredentialsProvider credentials = AWSUtil.getCredentialsProvider(connection.awsConfig,
        context,
        regions
    );
    ClientConfiguration clientConfig = AWSUtil.getClientConfiguration(connection.proxyConfig);

    if (maxErrorRetries >= 0) {
      clientConfig.setMaxErrorRetry(maxErrorRetries);
    }

    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                                                         .withCredentials(credentials)
                                                         .withClientConfiguration(clientConfig)
                                                         .withChunkedEncodingDisabled(connection.awsConfig.disableChunkedEncoding)
                                                         .withPathStyleAccessEnabled(usePathAddressModel);

    if (connection.useRegion) {
      if (connection.region == AwsRegion.OTHER) {
        if (connection.endpoint == null || connection.endpoint.isEmpty()) {
          issues.add(context.createConfigIssue(Groups.S3.name(), configPrefix + "endpoint", Errors.S3_SPOOLDIR_10));
          return null;
        }
        builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(connection.endpoint, null));
      } else {
        builder.withRegion(connection.region.getId());
      }
    } else {
      builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("s3.amazonaws.com", null));
    }
    return builder.build();
  }
}
