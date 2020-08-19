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
}
