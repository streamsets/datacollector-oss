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

import com.streamsets.datacollector.config.DatabricksConfig;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.aws.SseOption;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.lib.aws.AwsInstanceType;
import com.streamsets.pipeline.lib.googlecloud.GoogleCloudConfig;
import com.streamsets.pipeline.stage.lib.aws.AWSCredentialMode;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import com.streamsets.pipeline.stage.common.emr.EMRClusterConnection;
import com.streamsets.testing.pipeline.stage.TestUpgraderContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static com.streamsets.datacollector.creation.PipelineConfigUpgrader.SDC_OLD_EMR_CONFIG;
import static com.streamsets.datacollector.creation.PipelineConfigUpgrader.TRANSFORMER_EMR_CONFIG;

public class TestPipelineConfigUpgrader {

  private StageUpgrader upgrader;

  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() throws IllegalAccessException, InstantiationException {
    final StageDef pipelineStageDef = PipelineConfigBean.class.getAnnotation(StageDef.class);
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource(pipelineStageDef.upgraderDef());
    upgrader = new SelectorStageUpgrader("stage", pipelineStageDef.upgrader().newInstance(), yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testPipelineConfigUpgradeV1ToV7() throws StageException {
    PipelineConfigUpgrader pipelineConfigUpgrader = new PipelineConfigUpgrader();
    TestUpgraderContext context = new TestUpgraderContext("x", "y", "z", 1, 7);

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

    Assert.assertEquals(TRANSFORMER_EMR_CONFIG + ".serviceAccessSecurityGroup", upgrade.get(0).getName());
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

    String regex = (to == 10 ? SDC_OLD_EMR_CONFIG : TRANSFORMER_EMR_CONFIG) + ".";
    List<String> emrConfigList = upgraded.stream()
                                   .map(conf -> conf.getName().replaceAll(regex,""))
                                   .collect(Collectors.toList());

    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.ACCESS_KEY));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.SECRET_KEY));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.CLUSTER_ID));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.CLUSTER_PREFIX));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.EC2_SUBNET_ID));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.INSTANCE_COUNT));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.JOB_FLOW_ROLE));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.MASTER_INSTANCE_TYPE));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.MASTER_SECURITY_GROUP));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.MASTER_INSTANCE_TYPE_CUSTOM));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.SLAVE_INSTANCE_TYPE));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.SLAVE_SECURITY_GROUP));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.SLAVE_INSTANCE_TYPE_CUSTOM));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.PROVISION_NEW_CLUSTER));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.S3_LOG_URI));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.S3_STAGING_URI));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.SERVICE_ROLE));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.USER_REGION));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.USER_REGION_CUSTOM));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.TERMINATE_CLUSTER));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.VISIBLE_TO_ALL_USERS));
    Assert.assertTrue(emrConfigList.contains(EMRClusterConnection.LOGGING_ENABLED));

    if (to == 10) {
      List<Config> testOriginStageConfigList = upgraded.stream()
                                                 .filter(config -> config.getName().equals("testOriginStage"))
                                                 .collect(Collectors.toList());

      Assert.assertEquals(1, testOriginStageConfigList.size());
      Assert.assertEquals(PipelineConfigBean.RAW_DATA_ORIGIN, testOriginStageConfigList.get(0).getValue());
      Assert.assertTrue(emrConfigList.contains(PipelineConfigUpgrader.ENABLE_EMR_DEBUGGING_CONFIG_FIELD));
    }

    if (to == 17) {
      Assert.assertTrue(emrConfigList.contains("useIAMRoles"));
      Assert.assertTrue(emrConfigList.contains("clusterConfig.callbackUrl"));
    }
  }

  @Test
  public void testPipelineConfigUpgradeV19ToV20() {
    PipelineConfigUpgrader pipelineConfigUpgrader = new PipelineConfigUpgrader();
    TestUpgraderContext context = new TestUpgraderContext("x", "y", "z", 19, 20);
    List<Config> upgraded = pipelineConfigUpgrader.upgrade(new ArrayList<>(), context);

    Assert.assertEquals(2, upgraded.size());
    Assert.assertEquals(TRANSFORMER_EMR_CONFIG + ".encryption", upgraded.get(0).getName());
    Assert.assertEquals(SseOption.NONE, upgraded.get(0).getValue());
    Assert.assertEquals(TRANSFORMER_EMR_CONFIG + ".kmsKeyId", upgraded.get(1).getName());
    Assert.assertNull(upgraded.get(1).getValue());
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

  /**
   * Expected values for EMR cluster upgrades
   */
  static class ExpectedVals {
    static final String userRegion = "us-east-1";
    static final String userRegionCustom = "jeff-4";
    static final String accessKey = "myAccessKey";
    static final String secretKey = "mySecret";
    static final String s3StagingUri = "s3://my-bucket/staging-dir";
    static final boolean provisionNewCluster = false;
    static final String clusterId = "j-26ICQZBZQBEFV";
    static final String emrVersion = "5.13.0";
    static final String clusterPrefix = "j-26ICQZBZQBEFV";
    static final boolean terminateCluster = false;
    static final boolean loggingEnabled = true;
    static final String s3LogUri = "s3://my-bucket/log-dir";
    static final boolean enableEMRDebugging = true;
    static final String serviceRole = "role-A";
    static final String jobFlowRole = "role-B";
    static final boolean visibleToAllUsers = false;
    static final String ec2SubnetId = "subnet-0bb1c79de3EXAMPLE";
    static final String masterSecurityGroup = "sg-1";
    static final String slaveSecurityGroup = "sg-2";
    static final String serviceAccessSecurityGroup = "sg-3";
    static final boolean useIAMRoles = true;
    static final int instanceCount = 2;
    static final AwsInstanceType masterInstanceType = AwsInstanceType.R4_2XLARGE;
    static final String masterInstanceTypeCustom = "x4.16xhuge";
    static final AwsInstanceType slaveInstanceType = AwsInstanceType.C4_8XLARGE;
    static final String slaveInstanceTypeCustom = "x8.32xwow";
    static final String kmsKeyId = "arn:aws:kms:region:acct-id:key/blah";
    static final SseOption encryption = SseOption.KMS;
  }

  private static void assertEMRConfigToConnectionMoves() {
    // test upgrade of old EMR configs to new connection
    final List<Config> configs = new LinkedList<>();

    // add formerly SDC-specific configs that will be moved to the connection
    addCommonOldEMRConfigs(configs, PipelineConfigUpgrader.SDC_OLD_EMR_CONFIG);
    // add the SDC-specific, non-connection based property that would have already existed at this point
    configs.add(new Config(
        PipelineConfigUpgrader.SDC_OLD_EMR_CONFIG + ".enableEMRDebugging",
        ExpectedVals.enableEMRDebugging
    ));
    // add formerly Transformer-specific configs that will be moved to the connection
    addCommonOldEMRConfigs(configs, PipelineConfigUpgrader.TRANSFORMER_EMR_CONFIG);
    // serviceAccessSecurityGroup and emrVersion are special cases: Transformer-only, but moving to connection anyway
    configs.add(new Config(
        PipelineConfigUpgrader.TRANSFORMER_EMR_CONFIG + ".serviceAccessSecurityGroup",
        ExpectedVals.serviceAccessSecurityGroup
    ));
    configs.add(new Config(
        PipelineConfigUpgrader.TRANSFORMER_EMR_CONFIG + ".emrVersion",
        ExpectedVals.emrVersion
    ));
    // add the other Transformer-specific, non-connection based properties that would have already existed at this point
    configs.add(new Config(
        PipelineConfigUpgrader.TRANSFORMER_EMR_CONFIG + ".kmsKeyId",
        ExpectedVals.kmsKeyId
    ));
    configs.add(new Config(
        PipelineConfigUpgrader.TRANSFORMER_EMR_CONFIG + ".encryption",
        ExpectedVals.encryption
    ));
    configs.add(new Config(
        PipelineConfigUpgrader.TRANSFORMER_EMR_CONFIG + ".useIAMRoles",
        ExpectedVals.useIAMRoles
    ));

    final UpgraderTestUtils.UpgradeMoveWatcher watcher = UpgraderTestUtils.snapshot(configs);

    PipelineConfigUpgrader pipelineConfigUpgrader = new PipelineConfigUpgrader();
    TestUpgraderContext context = new TestUpgraderContext("x", "y", "z", 20, 21);

    final List<Config> upgraded = pipelineConfigUpgrader.upgrade(configs, context);

    // SDC-specific, connection fields should have been moved to their new homes
    assertCommonEMRConfigsMoved(
        watcher,
        configs,
        PipelineConfigUpgrader.SDC_OLD_EMR_CONFIG,
        PipelineConfigUpgrader.SDC_NEW_EMR_CONNECTION
    );

    // SDC-specific EMR field that should have been moved to the top level pipeline config
    watcher.assertAllMoved(
        configs,
        PipelineConfigUpgrader.SDC_OLD_EMR_CONFIG + "." + PipelineConfigUpgrader.ENABLE_EMR_DEBUGGING_CONFIG_FIELD,
        PipelineConfigUpgrader.ENABLE_EMR_DEBUGGING_CONFIG_FIELD
    );

    // the credentialMode should have been initialized with a default now
    UpgraderTestUtils.assertExists(
        upgraded,
        PipelineConfigUpgrader.SDC_NEW_EMR_CONNECTION + ".awsConfig.credentialMode",
        AWSCredentialMode.WITH_CREDENTIALS
    );

    // Transformer-specific, connection fields should have been moved to their new homes
    assertCommonEMRConfigsMoved(
        watcher,
        configs,
        PipelineConfigUpgrader.TRANSFORMER_EMR_CONFIG,
        PipelineConfigUpgrader.TRANSFORMER_NEW_EMR_CONNECTION
    );
    // the special cases of serviceAccessSecurityGroup and emrVersion; should have been moved to the connection
    UpgraderTestUtils.assertAllExist(
        upgraded,
        PipelineConfigUpgrader.TRANSFORMER_NEW_EMR_CONNECTION + ".serviceAccessSecurityGroup",
        PipelineConfigUpgrader.TRANSFORMER_NEW_EMR_CONNECTION + ".emrVersion"
    );

    // Transformer-specific, non connection fields should have stayed put
    UpgraderTestUtils.assertAllExist(
        upgraded,
        PipelineConfigUpgrader.TRANSFORMER_EMR_CONFIG + ".kmsKeyId",
        PipelineConfigUpgrader.TRANSFORMER_EMR_CONFIG + ".encryption"
    );
    // the useIAMRoles boolean should have morphed into an instance of AWSCredentialMode
    UpgraderTestUtils.assertExists(
        upgraded,
        PipelineConfigUpgrader.TRANSFORMER_NEW_EMR_CONNECTION + ".awsConfig.credentialMode",
        AWSCredentialMode.WITH_IAM_ROLES
    );
    // but the old config should no longer exist
    UpgraderTestUtils.assertNoneExist(
        upgraded,
        PipelineConfigUpgrader.TRANSFORMER_EMR_CONFIG + "." + PipelineConfigUpgrader.USE_IAM_ROLES_CONFIG_FIELD
    );
  }

  private static void addCommonOldEMRConfigs(List<Config> configs, String oldConfigField) {
    configs.add(new Config(oldConfigField + ".userRegion", ExpectedVals.userRegion));
    configs.add(new Config(oldConfigField + ".userRegionCustom", ExpectedVals.userRegionCustom));
    configs.add(new Config(oldConfigField + ".accessKey", ExpectedVals.accessKey));
    configs.add(new Config(oldConfigField + ".secretKey", ExpectedVals.secretKey));
    configs.add(new Config(oldConfigField + ".s3StagingUri", ExpectedVals.s3StagingUri));
    configs.add(new Config(oldConfigField + ".provisionNewCluster", ExpectedVals.provisionNewCluster));
    configs.add(new Config(oldConfigField + ".clusterId", ExpectedVals.clusterId));
    configs.add(new Config(oldConfigField + ".clusterPrefix", ExpectedVals.clusterPrefix));
    configs.add(new Config(oldConfigField + ".terminateCluster", ExpectedVals.terminateCluster));
    configs.add(new Config(oldConfigField + ".loggingEnabled", ExpectedVals.loggingEnabled));
    configs.add(new Config(oldConfigField + ".s3LogUri", ExpectedVals.s3LogUri));
    configs.add(new Config(oldConfigField + ".serviceRole", ExpectedVals.serviceRole));
    configs.add(new Config(oldConfigField + ".jobFlowRole", ExpectedVals.jobFlowRole));
    configs.add(new Config(oldConfigField + ".visibleToAllUsers", ExpectedVals.visibleToAllUsers));
    configs.add(new Config(oldConfigField + ".ec2SubnetId", ExpectedVals.ec2SubnetId));
    configs.add(new Config(oldConfigField + ".masterSecurityGroup", ExpectedVals.masterSecurityGroup));
    configs.add(new Config(oldConfigField + ".slaveSecurityGroup", ExpectedVals.slaveSecurityGroup));
    configs.add(new Config(oldConfigField + ".instanceCount", ExpectedVals.instanceCount));
    configs.add(new Config(oldConfigField + ".masterInstanceType", ExpectedVals.masterInstanceType));
    configs.add(new Config(oldConfigField + ".masterInstanceTypeCustom", ExpectedVals.masterInstanceTypeCustom));
    configs.add(new Config(oldConfigField + ".slaveInstanceType", ExpectedVals.slaveInstanceType));
    configs.add(new Config(oldConfigField + ".slaveInstanceTypeCustom", ExpectedVals.slaveInstanceTypeCustom));
  }

  private static void assertCommonEMRConfigsMoved(
      UpgraderTestUtils.UpgradeMoveWatcher watcher,
      List<Config> configs,
      String oldConfigField,
      String newConnectionField
  ) {
    watcher.assertAllMoved(
        configs,
        oldConfigField + ".userRegion", newConnectionField + ".region",
        oldConfigField + ".userRegionCustom", newConnectionField + ".customRegion",
        oldConfigField + ".accessKey", newConnectionField + ".awsConfig.awsAccessKeyId",
        oldConfigField + ".secretKey", newConnectionField + ".awsConfig.awsSecretAccessKey",
        oldConfigField + ".s3StagingUri", newConnectionField + ".s3StagingUri",
        oldConfigField + ".provisionNewCluster", newConnectionField + ".provisionNewCluster",
        oldConfigField + ".clusterId", newConnectionField + ".clusterId",
        oldConfigField + ".clusterPrefix", newConnectionField + ".clusterPrefix",
        oldConfigField + ".terminateCluster", newConnectionField + ".terminateCluster",
        oldConfigField + ".loggingEnabled", newConnectionField + ".loggingEnabled",
        oldConfigField + ".s3LogUri", newConnectionField + ".s3LogUri",
        oldConfigField + ".s3StagingUri", newConnectionField + ".s3StagingUri",
        oldConfigField + ".serviceRole", newConnectionField + ".serviceRole",
        oldConfigField + ".jobFlowRole", newConnectionField + ".jobFlowRole",
        oldConfigField + ".visibleToAllUsers", newConnectionField + ".visibleToAllUsers",
        oldConfigField + ".ec2SubnetId", newConnectionField + ".ec2SubnetId",
        oldConfigField + ".masterSecurityGroup", newConnectionField + ".masterSecurityGroup",
        oldConfigField + ".slaveSecurityGroup", newConnectionField + ".slaveSecurityGroup",
        oldConfigField + ".instanceCount", newConnectionField + ".instanceCount",
        oldConfigField + ".masterInstanceType", newConnectionField + ".masterInstanceType",
        oldConfigField + ".masterInstanceTypeCustom", newConnectionField + ".masterInstanceTypeCustom",
        oldConfigField + ".slaveInstanceType", newConnectionField + ".slaveInstanceType",
        oldConfigField + ".slaveInstanceTypeCustom", newConnectionField + ".slaveInstanceTypeCustom"
    );
  }

  @Test
  public void testPipelineConfigUpgradeV20ToV21() throws StageException {
    assertEMRConfigToConnectionMoves();
  }

  @Test
  public void testPipelineConfigUpgradeV21ToV22() throws StageException {
    PipelineConfigUpgrader pipelineConfigUpgrader = new PipelineConfigUpgrader();
    TestUpgraderContext context = new TestUpgraderContext("x", "y", "z", 21, 22);

    List<Config> upgrade = pipelineConfigUpgrader.upgrade(new ArrayList<>(), context);

    Assert.assertEquals("sdcEmrConnection.stepConcurrency", upgrade.get(0).getName());
    Assert.assertEquals(1, upgrade.get(0).getValue());
    Assert.assertEquals("transformerEmrConnection.stepConcurrency", upgrade.get(1).getName());
    Assert.assertEquals(1, upgrade.get(1).getValue());
  }
}
