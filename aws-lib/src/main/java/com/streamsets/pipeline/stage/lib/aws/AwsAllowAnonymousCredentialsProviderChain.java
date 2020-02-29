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
package com.streamsets.pipeline.stage.lib.aws;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsAllowAnonymousCredentialsProviderChain extends DefaultAWSCredentialsProviderChain {
  private static final Logger LOG = LoggerFactory.getLogger(AwsAllowAnonymousCredentialsProviderChain.class);

  @Override
  public AWSCredentials getCredentials() {
    try {
      return super.getCredentials();
    } catch (AmazonClientException e) {
      LOG.debug("No credentials found; using anonymous access");
      return new AnonymousAWSCredentials();
    }
  }
}
