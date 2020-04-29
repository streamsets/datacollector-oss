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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.junit.Assert;
import org.junit.Test;

public class TestAWSUtil {

  @Test
  public void testGetCredentialsProviderCredentials() {
    AWSConfig awsConfig = new AWSConfig();
    awsConfig.credentialMode = AWSCredentialMode.WITH_CREDENTIALS;
    awsConfig.awsAccessKeyId = () -> "abc";
    awsConfig.awsSecretAccessKey = () -> "xyz";

    AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(awsConfig);
    Assert.assertEquals("abc", credentialsProvider.getCredentials().getAWSAccessKeyId());
    Assert.assertEquals("xyz", credentialsProvider.getCredentials().getAWSSecretKey());
  }

  @Test
  public void testGetCredentialsProviderCredentialsEmpty() {
    AWSConfig awsConfig = new AWSConfig();
    awsConfig.credentialMode = AWSCredentialMode.WITH_CREDENTIALS;
    // The FE should prevent this case, but if it somehow happens, we'll fallback to the Default provider
    awsConfig.awsAccessKeyId = () -> "";
    awsConfig.awsSecretAccessKey = () -> "";

    AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(awsConfig);
    Assert.assertEquals(DefaultAWSCredentialsProviderChain.getInstance(), credentialsProvider);
  }

  @Test
  public void testGetCredentialsProviderIAM() {
    AWSConfig awsConfig = new AWSConfig();
    awsConfig.credentialMode = AWSCredentialMode.WITH_IAM_ROLES;

    AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(awsConfig);
    Assert.assertEquals(DefaultAWSCredentialsProviderChain.getInstance(), credentialsProvider);
  }

  @Test
  public void testGetCredentialsProviderAnonymous() {
    AWSConfig awsConfig = new AWSConfig();
    awsConfig.credentialMode = AWSCredentialMode.WITH_ANONYMOUS_CREDENTIALS;

    AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(awsConfig);
    Assert.assertNull(credentialsProvider.getCredentials().getAWSAccessKeyId());
    Assert.assertNull(credentialsProvider.getCredentials().getAWSSecretKey());
  }
}
