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
package com.streamsets.pipeline.stage.origin.sqs;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class SqsDSourceUpgraderTest {
  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/SqsDSource.yaml");
    upgrader = new SelectorStageUpgrader("stage", null, yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testV3toV4BothEmptyCredentials() throws StageException {
    configs.add(new Config("sqsConfig.awsConfig.awsAccessKeyId", ""));
    configs.add(new Config("sqsConfig.awsConfig.awsSecretAccessKey", ""));
    testV3toV4("WITH_IAM_ROLES");
  }

  @Test
  public void testV3toV4FirstEmptyCredentials() throws StageException {
    configs.add(new Config("sqsConfig.awsConfig.awsAccessKeyId", ""));
    configs.add(new Config("sqsConfig.awsConfig.awsSecretAccessKey", "foo"));
    testV3toV4("WITH_CREDENTIALS");
  }

  @Test
  public void testV3toV4SecondEmptyCredentials() throws StageException {
    configs.add(new Config("sqsConfig.awsConfig.awsAccessKeyId", "foo"));
    configs.add(new Config("sqsConfig.awsConfig.awsSecretAccessKey", ""));
    testV3toV4("WITH_CREDENTIALS");
  }

  @Test
  public void testV3toV4NoneEmptyCredentials() throws StageException {
    configs.add(new Config("sqsConfig.awsConfig.awsAccessKeyId", "foo"));
    configs.add(new Config("sqsConfig.awsConfig.awsSecretAccessKey", "bar"));
    testV3toV4("WITH_CREDENTIALS");
  }

  private void testV3toV4(String expectedCredentialsMode) throws StageException {
    Mockito.doReturn(3).when(context).getFromVersion();
    Mockito.doReturn(4).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "sqsConfig.awsConfig.credentialMode", expectedCredentialsMode);

  }

  @Test
  public void testV4toV5() throws StageException {
    Mockito.doReturn(4).when(context).getFromVersion();
    Mockito.doReturn(5).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "sqsConfig.specifyQueueURL", false);
    UpgraderTestUtils.assertExists(configs, "sqsConfig.queueUrls", new ArrayList<>());
  }

  @Test
  public void testV5toV6() throws StageException {
    Mockito.doReturn(5).when(context).getFromVersion();
    Mockito.doReturn(6).when(context).getToVersion();

    configs.add(new Config("sqsConfig.awsConfig.awsAccessKeyId", "v1"));
    configs.add(new Config("sqsConfig.awsConfig.awsSecretAccessKey", "v2"));
    configs.add(new Config("sqsConfig.region", "v3"));
    configs.add(new Config("sqsConfig.endpoint", "v4"));
    configs.add(new Config("sqsConfig.proxyConfig.connectionTimeout", "v5"));
    configs.add(new Config("sqsConfig.proxyConfig.socketTimeout", "v6"));
    configs.add(new Config("sqsConfig.proxyConfig.retryCount", "v7"));
    configs.add(new Config("sqsConfig.proxyConfig.useProxy", "v8"));
    configs.add(new Config("sqsConfig.proxyConfig.proxyHost", "v9"));
    configs.add(new Config("sqsConfig.proxyConfig.proxyPort", "v10"));
    configs.add(new Config("sqsConfig.proxyConfig.proxyUser", "v11"));
    configs.add(new Config("sqsConfig.proxyConfig.proxyPassword", "v12"));
    configs.add(new Config("sqsConfig.proxyConfig.proxyDomain", "v13"));
    configs.add(new Config("sqsConfig.proxyConfig.proxyWorkstation", "v14"));

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs,"sqsConfig.connection.awsConfig.awsAccessKeyId", "v1");
    UpgraderTestUtils.assertExists(configs,"sqsConfig.connection.awsConfig.awsSecretAccessKey", "v2");
    UpgraderTestUtils.assertExists(configs,"sqsConfig.connection.region", "v3");
    UpgraderTestUtils.assertExists(configs,"sqsConfig.connection.endpoint", "v4");
    UpgraderTestUtils.assertExists(configs,"sqsConfig.connection.proxyConfig.connectionTimeout", "v5");
    UpgraderTestUtils.assertExists(configs,"sqsConfig.connection.proxyConfig.socketTimeout", "v6");
    UpgraderTestUtils.assertExists(configs,"sqsConfig.connection.proxyConfig.retryCount", "v7");
    UpgraderTestUtils.assertExists(configs,"sqsConfig.connection.proxyConfig.useProxy", "v8");
    UpgraderTestUtils.assertExists(configs,"sqsConfig.connection.proxyConfig.proxyHost", "v9");
    UpgraderTestUtils.assertExists(configs,"sqsConfig.connection.proxyConfig.proxyPort", "v10");
    UpgraderTestUtils.assertExists(configs,"sqsConfig.connection.proxyConfig.proxyUser", "v11");
    UpgraderTestUtils.assertExists(configs,"sqsConfig.connection.proxyConfig.proxyPassword", "v12");
    UpgraderTestUtils.assertExists(configs,"sqsConfig.connection.proxyConfig.proxyDomain", "v13");
    UpgraderTestUtils.assertExists(configs,"sqsConfig.connection.proxyConfig.proxyWorkstation", "v14");

    Assert.assertEquals(14, configs.size());

  }
}
