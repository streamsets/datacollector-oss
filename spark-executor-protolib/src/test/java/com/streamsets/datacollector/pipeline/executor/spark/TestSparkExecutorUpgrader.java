/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.pipeline.executor.spark;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSparkExecutorUpgrader {

  @Test
  public void testV1ToV2() throws StageException {
    SparkExecutorUpgrader upgrader = new SparkExecutorUpgrader();
    StageUpgrader.Context context = mock(StageUpgrader.Context.class);
    when(context.getFromVersion()).thenReturn(1);

    List<Config> configs = new ArrayList<>();
    final Config configToRetain1 = new Config("conf.yarnConfigBean.testing", 1);
    final Config ConfigToRetain2 = new Config("conf.sparkHome", "/home/spark");
    configs.add(configToRetain1);
    configs.add(ConfigToRetain2);
    configs.add(new Config("conf.databricksConfigBean.baseUri", "https://test.com/"));
    configs.add(new Config("conf.databricksConfigBean.jobId", 404));
    configs.add(new Config("conf.credentialsConfigBean.username", "username"));
    configs.add(new Config("conf.credentialsConfigBean.password", "password"));
    configs.add(new Config("conf.clusterManager", "YARN"));

    List<Config> upgraded = upgrader.upgrade(configs, context);

    Assert.assertEquals(2, upgraded.size());
    Assert.assertTrue(upgraded.contains(configToRetain1));
    Assert.assertTrue(upgraded.contains(ConfigToRetain2));

  }

  @Test
  public void testV2toV3() throws StageException {
    List<Config> configs = new ArrayList<>();
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/SparkDExecutor.yaml");

    StageUpgrader upgrader = new SelectorStageUpgrader("stage", new SparkExecutorUpgrader(), yamlResource);

    StageUpgrader.Context  context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(2).when(context).getFromVersion();
    Mockito.doReturn(3).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);
    //conf.yarnConfigBean.submitTimeout
    UpgraderTestUtils.assertExists(configs, "conf.yarnConfigBean.submitTimeout", 0);


  }

}
