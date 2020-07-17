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
import com.streamsets.datacollector.config.DatabricksConfig;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.googlecloud.GoogleCloudConfig;
import com.streamsets.testing.pipeline.stage.TestUpgraderContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
    doTestEMRConfigs(9, 10);
  }

  @Test
  public void testPipelineConfigUpgradeV12ToV13() throws StageException {
    PipelineConfigUpgrader pipelineConfigUpgrader = new PipelineConfigUpgrader();
    TestUpgraderContext context = new TestUpgraderContext("x", "y", "z", 12, 13);

    List<Config> upgrade = pipelineConfigUpgrader.upgrade(new ArrayList<>(), context);

    Assert.assertEquals("clusterConfig.clusterType", upgrade.get(0).getName());
    Assert.assertEquals("LOCAL", upgrade.get(0).getValue());
    Assert.assertEquals("clusterConfig.sparkMasterUrl", upgrade.get(1).getName());
    Assert.assertEquals("local[*]", upgrade.get(1).getValue());
    Assert.assertEquals("clusterConfig.deployMode", upgrade.get(2).getName());
    Assert.assertEquals("CLIENT", upgrade.get(2).getValue());
    Assert.assertEquals("clusterConfig.hadoopUserName", upgrade.get(3).getName());
    Assert.assertEquals("hdfs", upgrade.get(3).getValue());
    Assert.assertEquals("clusterConfig.stagingDir", upgrade.get(4).getName());
    Assert.assertEquals("/streamsets", upgrade.get(4).getValue());

    Assert.assertEquals("databricksConfig.baseUrl", upgrade.get(5).getName());
    Assert.assertNull(upgrade.get(5).getValue());
    Assert.assertEquals("databricksConfig.credentialType", upgrade.get(6).getName());
    Assert.assertNull(upgrade.get(6).getValue());
    Assert.assertEquals("databricksConfig.username", upgrade.get(7).getName());
    Assert.assertNull(upgrade.get(7).getValue());
    Assert.assertEquals("databricksConfig.password", upgrade.get(8).getName());
    Assert.assertNull(upgrade.get(8).getValue());
    Assert.assertEquals("databricksConfig.token", upgrade.get(9).getName());
    Assert.assertNull(upgrade.get(9).getValue());
    Assert.assertEquals("databricksConfig.clusterConfig", upgrade.get(10).getName());
    Assert.assertEquals(DatabricksConfig.DEFAULT_CLUSTER_CONFIG, upgrade.get(10).getValue());

    Assert.assertEquals("livyConfig.baseUrl", upgrade.get(11).getName());
    Assert.assertNull(upgrade.get(11).getValue());
    Assert.assertEquals("livyConfig.username", upgrade.get(12).getName());
    Assert.assertNull(upgrade.get(12).getValue());
    Assert.assertEquals("livyConfig.password", upgrade.get(13).getName());
    Assert.assertNull(upgrade.get(13).getValue());
  }

  @Test
  public void testPipelineConfigUpgradeV13ToV14() throws StageException {
    PipelineConfigUpgrader pipelineConfigUpgrader = new PipelineConfigUpgrader();
    TestUpgraderContext context = new TestUpgraderContext("x", "y", "z", 13, 14);

    List<Config> upgrade = pipelineConfigUpgrader.upgrade(new ArrayList<>(), context);

    Assert.assertEquals("databricksConfig.provisionNewCluster", upgrade.get(0).getName());
    Assert.assertEquals(upgrade.get(0).getValue(), true);
    Assert.assertEquals("databricksConfig.clusterId", upgrade.get(1).getName());
    Assert.assertNull(upgrade.get(1).getValue());
    Assert.assertEquals("databricksConfig.terminateCluster", upgrade.get(2).getName());
    Assert.assertEquals(upgrade.get(2).getValue(), false);
  }


  @Test
  public void testPipelineConfigUpgradeV14ToV15() throws StageException {
    PipelineConfigUpgrader pipelineConfigUpgrader = new PipelineConfigUpgrader();
    TestUpgraderContext context = new TestUpgraderContext("x", "y", "z", 14, 15);

    List<Config> upgrade = pipelineConfigUpgrader.upgrade(new ArrayList<>(), context);

    Assert.assertEquals("ludicrousMode", upgrade.get(0).getName());
    Assert.assertEquals(false, upgrade.get(0).getValue());
    Assert.assertEquals("ludicrousModeInputCount", upgrade.get(1).getName());
    Assert.assertEquals(false, upgrade.get(1).getValue());
    Assert.assertEquals("advancedErrorHandling", upgrade.get(2).getName());
    Assert.assertEquals(false, upgrade.get(2).getValue());
    Assert.assertEquals("triggerInterval", upgrade.get(3).getName());
    Assert.assertEquals(2000, upgrade.get(3).getValue());
  }

  @Test
  public void testPipelineConfigUpgradeV15ToV16() throws StageException {
    PipelineConfigUpgrader pipelineConfigUpgrader = new PipelineConfigUpgrader();
    TestUpgraderContext context = new TestUpgraderContext("x", "y", "z", 15, 16);

    List<Config> upgrade = pipelineConfigUpgrader.upgrade(new ArrayList<>(), context);

    Assert.assertEquals("preprocessScript", upgrade.get(0).getName());
    Assert.assertEquals("", upgrade.get(0).getValue());
  }

  @Test
  public void testPipelineConfigUpgradeV16ToV17() throws StageException {
    doTestEMRConfigs(16, 17);
  }

  @Test
  public void testPipelineConfigUpgradeV17ToV18() throws StageException {
    PipelineConfigUpgrader pipelineConfigUpgrader = new PipelineConfigUpgrader();
    TestUpgraderContext context = new TestUpgraderContext("x", "y", "z", 17, 18);

    List<Config> upgrade = pipelineConfigUpgrader.upgrade(new ArrayList<>(), context);

    Assert.assertEquals("transformerEMRConfig.serviceAccessSecurityGroup", upgrade.get(0).getName());
    Assert.assertNull(upgrade.get(0).getValue());
  }

  @Test
  public void testPipelineConfigUpgradeV18ToV19() throws StageException {
    doTestDataprocConfigs(18, 19);
  }

  private void doTestEMRConfigs(int from, int to) {
    PipelineConfigUpgrader pipelineConfigUpgrader = new PipelineConfigUpgrader();
    TestUpgraderContext context = new TestUpgraderContext("x", "y", "z", from, to);
    List<Config> upgraded = pipelineConfigUpgrader.upgrade(new ArrayList<>(), context);

    String regex = to == 10 ? "amazonEMRConfig." : "transformerEMRConfig.";
    List<String> emrConfigList = upgraded.stream()
                                   .map(conf -> conf.getName().replaceAll(regex,""))
                                   .collect(Collectors.toList());

    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.ACCESS_KEY));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.SECRET_KEY));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.CLUSTER_ID));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.CLUSTER_PREFIX));
    Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.EC2_SUBNET_ID));
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

    if (to == 10) {
      List<Config> testOriginStageConfigList = upgraded.stream()
                                                 .filter(config -> config.getName().equals("testOriginStage"))
                                                 .collect(Collectors.toList());

      Assert.assertEquals(1, testOriginStageConfigList.size());
      Assert.assertEquals(PipelineConfigBean.RAW_DATA_ORIGIN, testOriginStageConfigList.get(0).getValue());
      Assert.assertTrue(emrConfigList.contains(AmazonEMRConfig.ENABLE_EMR_DEBUGGING));
    }

    if (to == 17) {
      Assert.assertTrue(emrConfigList.contains("useIAMRoles"));
      Assert.assertTrue(emrConfigList.contains("clusterConfig.callbackUrl"));
    }
  }

  private void doTestDataprocConfigs(int from, int to) {
    PipelineConfigUpgrader pipelineConfigUpgrader = new PipelineConfigUpgrader();
    TestUpgraderContext context = new TestUpgraderContext("x", "y", "z", from, to);
    List<Config> upgraded = pipelineConfigUpgrader.upgrade(new ArrayList<>(), context);

    String regex = "googleCloudConfig.";
    String regex2 = "googleCloudCredentialsConfig.";
    List<Config> gcloudConfigList = upgraded.stream()
                                      .map(conf -> new Config(conf.getName().replaceAll(regex, ""), conf.getValue()))
                                      .map(conf -> new Config(conf.getName().replaceAll(regex2, ""), conf.getValue()))
                                      .collect(Collectors.toList());
    Iterator<Config> iter = gcloudConfigList.iterator();
    assertInIter(iter, "region", null);
    assertInIter(iter, "customRegion", null);
    assertInIter(iter, "gcsStagingUri", null);
    assertInIter(iter, "create", false);
    assertInIter(iter, "clusterPrefix", null);
    assertInIter(iter, "version", GoogleCloudConfig.DATAPROC_IMAGE_VERSION_DEFAULT);
    assertInIter(iter, "masterType", null);
    assertInIter(iter, "workerType", null);
    assertInIter(iter, "networkType", null);
    assertInIter(iter, "network", null);
    assertInIter(iter, "subnet", null);
    assertInIter(iter, "tags", new ArrayList<String>());
    assertInIter(iter, "workerCount", 2);
    assertInIter(iter, "clusterName", null);
    assertInIter(iter, "terminate", false);

    assertInIter(iter, "projectId", null);
    assertInIter(iter, "credentialsProvider", null);
    assertInIter(iter, "path", null);
    assertInIter(iter, "credentialsFileContent", null);
  }

  private void assertInIter(Iterator<Config> conf, String key, Object value) {
    Assert.assertTrue(conf.hasNext());
    Config next = conf.next();
    Assert.assertEquals(next.getName(), key);
    Assert.assertEquals(next.getValue(), value);
  }
}
