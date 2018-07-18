/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.creation;

import com.streamsets.datacollector.config.AmazonEMRConfig;
import com.streamsets.datacollector.config.LogLevel;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.testing.pipeline.stage.TestUpgraderContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestPipelineConfigUpgrader {

  @Test
  public void testPipelineConfigUpgrader() throws StageException {
    PipelineConfigUpgrader pipelineConfigUpgrader = new PipelineConfigUpgrader();
    TestUpgraderContext context = new TestUpgraderContext("x", "y", "z", 1, 3);

    List<Config> upgrade = pipelineConfigUpgrader.upgrade(new ArrayList<>(), context);
    Assert.assertEquals("executionMode", upgrade.get(0).getName());
    Assert.assertEquals(ExecutionMode.STANDALONE, upgrade.get(0).getValue());

    Assert.assertEquals(false, upgrade.get(1).getValue());
    Assert.assertEquals(-1, upgrade.get(2).getValue());
    Assert.assertNotNull(upgrade.get(3).getValue());
    Assert.assertNotNull(upgrade.get(4).getValue());

    Assert.assertEquals("statsAggregatorStage", upgrade.get(5).getName());
    Assert.assertNull(upgrade.get(5).getValue());
    Assert.assertEquals("workerCount", upgrade.get(7).getName());
    Assert.assertEquals(0,  upgrade.get(7).getValue());
  }


  @Test
  public void testPipelineConfigUpgradeV7ToV8() throws StageException {
    PipelineConfigUpgrader pipelineConfigUpgrader = new PipelineConfigUpgrader();

    //check Write to dpm straightly correctly upgraded
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("executionMode", ExecutionMode.CLUSTER_YARN_STREAMING));
    configs.add(new Config("statsAggregatorStage", PipelineConfigBean.STATS_DPM_DIRECTLY_TARGET));

    TestUpgraderContext context = new TestUpgraderContext("x", "y", "z", 7, 8);
    List<Config> upgraded = pipelineConfigUpgrader.upgrade(configs, context);

    List<Config> statsAggregatorConfigList = upgraded.stream().filter(config -> config.getName().equals("statsAggregatorStage")).collect(
        Collectors.toList());

    Assert.assertEquals(1, statsAggregatorConfigList.size());
    Assert.assertEquals(PipelineConfigBean.STATS_AGGREGATOR_DEFAULT, statsAggregatorConfigList.get(0).getValue());

    // check Non Write to DPM Straightly is not affected
    String STATS_SDC_RPC = "streamsets-datacollector-basic-lib::com.streamsets.pipeline.stage.destination.sdcipc.StatsSdcIpcDTarget::2";
    configs = new ArrayList<>();
    configs.add(new Config("executionMode", ExecutionMode.CLUSTER_YARN_STREAMING));
    configs.add(new Config("statsAggregatorStage", STATS_SDC_RPC));

    upgraded = pipelineConfigUpgrader.upgrade(configs, context);

    statsAggregatorConfigList = upgraded.stream().filter(config -> config.getName().equals("statsAggregatorStage")).collect(
        Collectors.toList());

    Assert.assertEquals(1, statsAggregatorConfigList.size());
    Assert.assertEquals(STATS_SDC_RPC, statsAggregatorConfigList.get(0).getValue());
  }

  @Test
  public void testPipelineConfigUpgradeV8ToV9() throws StageException {
    PipelineConfigUpgrader pipelineConfigUpgrader = new PipelineConfigUpgrader();

    TestUpgraderContext context = new TestUpgraderContext("x", "y", "z", 8, 9);
    List<Config> upgraded = pipelineConfigUpgrader.upgrade(new ArrayList<>(), context);

    List<Config> edgeHttpUrlConfigList = upgraded.stream()
        .filter(config -> config.getName().equals("edgeHttpUrl"))
        .collect(Collectors.toList());

    Assert.assertEquals(1, edgeHttpUrlConfigList.size());
    Assert.assertEquals(PipelineConfigBean.EDGE_HTTP_URL_DEFAULT, edgeHttpUrlConfigList.get(0).getValue());
  }

  @Test
  public void testPipelineConfigUpgradeV9ToV10() throws StageException {
    PipelineConfigUpgrader pipelineConfigUpgrader = new PipelineConfigUpgrader();
    TestUpgraderContext context = new TestUpgraderContext("x", "y", "z", 9, 10);
    List<Config> upgraded = pipelineConfigUpgrader.upgrade(new ArrayList<>(), context);

    List<Config> testOriginStageConfigList = upgraded.stream()
        .filter(config -> config.getName().equals("testOriginStage"))
        .collect(Collectors.toList());

    Assert.assertEquals(1, testOriginStageConfigList.size());
    Assert.assertEquals(PipelineConfigBean.RAW_DATA_ORIGIN, testOriginStageConfigList.get(0).getValue());

    List<Config> logLevelConfigList = upgraded.stream().filter(config -> config.getName().equals("logLevel")).collect(
        Collectors.toList());

    Assert.assertEquals(1, logLevelConfigList.size());
    Assert.assertEquals(LogLevel.INFO.getLabel(), logLevelConfigList.get(0).getValue());

    List<String> emrConfigList = upgraded.stream()
        .map(conf -> conf.getName().replaceAll("amazonEMRConfig.",""))
        .collect(Collectors.toList());

    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.ACCESS_KEY));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.SECRET_KEY));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.CLUSTER_ID));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.CLUSTER_PREFIX));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.EC2_SUBNET_ID));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.ENABLE_EMR_DEBUGGING));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.INSTANCE_COUNT));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.JOB_FLOW_ROLE));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.MASTER_INSTANCE_TYPE));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.MASTER_SECURITY_GROUP));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.MASTER_INSTANCE_TYPE_CUSTOM));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.SLAVE_INSTANCE_TYPE));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.SLAVE_SECURITY_GROUP));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.SLAVE_INSTANCE_TYPE_CUSTOM));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.PROVISION_NEW_CLUSTER));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.S3_LOG_URI));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.S3_STAGING_URI));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.SERVICE_ROLE));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.USER_REGION));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.USER_REGION_CUSTOM));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.TERMINATE_CLUSTER));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.VISIBLE_TO_ALL_USERS));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.LOGGING_ENABLED));
  }

}
