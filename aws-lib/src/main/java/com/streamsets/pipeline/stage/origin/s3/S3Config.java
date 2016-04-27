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
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.common.InterfaceAudience;
import com.streamsets.pipeline.common.InterfaceStability;
import com.streamsets.pipeline.stage.lib.aws.AWSRegionChooserValues;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
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
  public Regions region;

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

  //Undocumented configuration used only by the unit tests to point AmazonS3Client to the fakes3 server
  private String endPoint;

  public void setEndPointForTest(String endPoint) {
    this.endPoint = endPoint;
  }

  public void init(
      Stage.Context context,
      String configPrefix,
      ProxyConfig proxyConfig,
      List<Stage.ConfigIssue> issues
  ) {
    validateConnection(context, configPrefix, proxyConfig, issues);
    //if the commonPrefix does not end with delimiter, add one
    if (commonPrefix != null && !commonPrefix.isEmpty() && !commonPrefix.endsWith(delimiter)) {
      commonPrefix = commonPrefix + delimiter;
    }
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
      List<Stage.ConfigIssue> issues
  ) {
    AWSCredentialsProvider credentials = AWSUtil.getCredentialsProvider(awsConfig);
    ClientConfiguration clientConfig = AWSUtil.getClientConfiguration(proxyConfig);

    s3Client = new AmazonS3Client(credentials, clientConfig);
    s3Client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
    if(endPoint != null && !endPoint.isEmpty()) {
      s3Client.setEndpoint(endPoint);
    } else {
      s3Client.setRegion(Region.getRegion(region));
    }
    try {
      //check if the credentials are right by trying to list buckets
      s3Client.listBuckets();
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
