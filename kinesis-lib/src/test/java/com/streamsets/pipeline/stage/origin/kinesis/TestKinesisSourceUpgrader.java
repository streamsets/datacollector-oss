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

package com.streamsets.pipeline.stage.origin.kinesis;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.stage.lib.aws.AWSCredentialMode;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestKinesisSourceUpgrader {

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/KinesisDSource.yaml");
    upgrader = new SelectorStageUpgrader("stage", new KinesisSourceUpgrader(), yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testV6ToV7() {
    Mockito.doReturn(6).when(context).getFromVersion();
    Mockito.doReturn(7).when(context).getToVersion();

    String dataFormatPrefix = "kinesisConfig.dataFormatConfig.";
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, dataFormatPrefix + "preserveRootElement", false);
  }

  @Test
  public void testUpgradeV7toV8() {
    KinesisSourceUpgrader kinesisSourceUpgrader = new KinesisSourceUpgrader();

    List<Config> configs = new ArrayList<>();
    configs.add(new Config("kinesisConfig.awsConfig.awsAccessKeyId", null));
    configs.add(new Config("kinesisConfig.awsConfig.awsSecretAccessKey", null));
    kinesisSourceUpgrader.upgrade("a", "b", "c", 7, 8, configs);
    assertEquals("kinesisConfig.awsConfig.credentialMode", configs.get(2).getName());
    assertEquals(AWSCredentialMode.WITH_IAM_ROLES, configs.get(2).getValue());

    configs = new ArrayList<>();
    configs.add(new Config("kinesisConfig.awsConfig.awsAccessKeyId", "key"));
    configs.add(new Config("kinesisConfig.awsConfig.awsSecretAccessKey", "secret"));
    kinesisSourceUpgrader.upgrade("a", "b", "c", 7, 8, configs);
    assertEquals("kinesisConfig.awsConfig.credentialMode", configs.get(2).getName());
    assertEquals(AWSCredentialMode.WITH_CREDENTIALS, configs.get(2).getValue());
  }

  @Test
  public void testUpgradeV9toV10() {
    Mockito.doReturn(9).when(context).getFromVersion();
    Mockito.doReturn(10).when(context).getToVersion();

    configs.add(new Config("kinesisConfig.kinesisConsumerConfigs", new HashMap<String, String>().put("", "")));
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "kinesisConfig.kinesisConsumerConfigs");
  }
}
