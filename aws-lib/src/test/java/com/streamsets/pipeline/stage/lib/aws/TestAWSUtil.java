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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.streamsets.pipeline.api.Stage;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestAWSUtil {

  @Test
  public void testGetCredentialsProviderCredentials() {
    AWSConfig awsConfig = new AWSConfig();
    awsConfig.credentialMode = AWSCredentialMode.WITH_CREDENTIALS;
    awsConfig.awsAccessKeyId = () -> "abc";
    awsConfig.awsSecretAccessKey = () -> "xyz";

    AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(awsConfig,
        Mockito.mock(Stage.Context.class),
        Regions.DEFAULT_REGION
    );
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

    AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(
        awsConfig,
        Mockito.mock(Stage.Context.class),
        Regions.DEFAULT_REGION
    );
    Assert.assertEquals(DefaultAWSCredentialsProviderChain.getInstance(), credentialsProvider);
  }

  @Test
  public void testGetCredentialsProviderIAM() {
    AWSConfig awsConfig = new AWSConfig();
    awsConfig.credentialMode = AWSCredentialMode.WITH_IAM_ROLES;

    AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(
        awsConfig,
        Mockito.mock(Stage.Context.class),
        Regions.DEFAULT_REGION
    );
    Assert.assertEquals(DefaultAWSCredentialsProviderChain.getInstance(), credentialsProvider);
  }

  @Test
  public void testGetCredentialsProviderAnonymous() {
    AWSConfig awsConfig = new AWSConfig();
    awsConfig.credentialMode = AWSCredentialMode.WITH_ANONYMOUS_CREDENTIALS;

    AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(
        awsConfig,
        Mockito.mock(Stage.Context.class),
        Regions.DEFAULT_REGION
    );
    Assert.assertTrue(AWSStaticCredentialsProvider.class.equals(credentialsProvider.getClass()));
    Assert.assertTrue(AnonymousAWSCredentials.class.equals(credentialsProvider.getCredentials().getClass()));
    Assert.assertNull(credentialsProvider.getCredentials().getAWSAccessKeyId());
    Assert.assertNull(credentialsProvider.getCredentials().getAWSSecretKey());
  }

  @Test
  public void testGetClientConfiguration() {
    ProxyConfig proxyConfig = new ProxyConfig();
    proxyConfig.useProxy = true;
    proxyConfig.proxyHost = "host";
    proxyConfig.proxyPort = 1234;
    proxyConfig.proxyUser = () -> "user";
    proxyConfig.proxyPassword = () -> "password";
    proxyConfig.proxyDomain = "domain";
    proxyConfig.proxyWorkstation = "workstation";

    ClientConfiguration clientConfig = AWSUtil.getClientConfiguration(proxyConfig);
    Assert.assertEquals("host", clientConfig.getProxyHost());
    Assert.assertEquals(1234, clientConfig.getProxyPort());
    Assert.assertEquals("user", clientConfig.getProxyUsername());
    Assert.assertEquals("password", clientConfig.getProxyPassword());
    Assert.assertEquals("domain", clientConfig.getProxyDomain());
    Assert.assertEquals("workstation", clientConfig.getProxyWorkstation());
  }

  @Test
  public void testGetClientConfigurationNotSet() {
    ProxyConfig proxyConfig = new ProxyConfig();
    proxyConfig.useProxy = true;

    ClientConfiguration clientConfig = AWSUtil.getClientConfiguration(proxyConfig);
    Assert.assertNull(clientConfig.getProxyHost());
    Assert.assertEquals(-1, clientConfig.getProxyPort());
    Assert.assertNull(clientConfig.getProxyUsername());
    Assert.assertNull(clientConfig.getProxyPassword());
    Assert.assertNull(clientConfig.getProxyDomain());
    Assert.assertNull(clientConfig.getProxyWorkstation());
  }

  @Test
  public void testGetClientConfigurationEmpty() {
    ProxyConfig proxyConfig = new ProxyConfig();
    proxyConfig.useProxy = true;
    proxyConfig.proxyHost = "";
    proxyConfig.proxyUser = () -> "";
    proxyConfig.proxyPassword = () -> "";
    proxyConfig.proxyDomain = "";
    proxyConfig.proxyWorkstation = "";

    ClientConfiguration clientConfig = AWSUtil.getClientConfiguration(proxyConfig);
    Assert.assertNull(clientConfig.getProxyHost());
    Assert.assertEquals(-1, clientConfig.getProxyPort());
    Assert.assertNull(clientConfig.getProxyUsername());
    Assert.assertNull(clientConfig.getProxyPassword());
    Assert.assertNull(clientConfig.getProxyDomain());
    Assert.assertNull(clientConfig.getProxyWorkstation());
  }

  @Test
  public void testGetClientConfigurationNotUsing() {
    ProxyConfig proxyConfig = new ProxyConfig();
    proxyConfig.useProxy = false; // other values will be ignored because this is false
    proxyConfig.proxyHost = "host";
    proxyConfig.proxyPort = 1234;
    proxyConfig.proxyUser = () -> "user";
    proxyConfig.proxyPassword = () -> "password";
    proxyConfig.proxyDomain = "domain";
    proxyConfig.proxyWorkstation = "workstation";

    ClientConfiguration clientConfig = AWSUtil.getClientConfiguration(proxyConfig);
    Assert.assertNull(clientConfig.getProxyHost());
    Assert.assertEquals(-1, clientConfig.getProxyPort());
    Assert.assertNull(clientConfig.getProxyUsername());
    Assert.assertNull(clientConfig.getProxyPassword());
    Assert.assertNull(clientConfig.getProxyDomain());
    Assert.assertNull(clientConfig.getProxyWorkstation());
  }
}
