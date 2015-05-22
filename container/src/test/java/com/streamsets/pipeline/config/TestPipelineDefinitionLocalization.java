/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.api.impl.LocaleInContext;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class TestPipelineDefinitionLocalization {

  @After
  public void cleanUp() {
    LocaleInContext.set(null);
  }

  private void testMessages(Locale locale, Map<String, String> expected) {
    RuntimeInfo runtimeInfo = new RuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
      Arrays.asList(TestPipelineDefinitionLocalization.class.getClassLoader()));
    PipelineDefinition def = new PipelineDefinition(runtimeInfo);
    LocaleInContext.set(locale);
    def = def.localize();

    //pipeline groups
    Assert.assertEquals(expected.get("pipelineConstantsGroup"), def.getConfigGroupDefinition().
      getGroupNameToLabelMapList().get(0).get("label"));

    Assert.assertEquals(expected.get("pipelineErrorRecordsGroup"), def.getConfigGroupDefinition().
      getGroupNameToLabelMapList().get(1).get("label"));
    Assert.assertEquals(expected.get("pipelineClusterGroup"), def.getConfigGroupDefinition().
        getGroupNameToLabelMapList().get(2).get("label"));

    //pipeline configs
    Assert.assertEquals(expected.get("executionModeLabel"), def.getConfigDefinitions().get(0).getLabel());
    Assert.assertEquals(expected.get("executionModeDescription"),
                        def.getConfigDefinitions().get(0).getDescription());
    Assert.assertEquals(expected.get("deliveryGuaranteeLabel"), def.getConfigDefinitions().get(1).getLabel());
    Assert.assertEquals(expected.get("deliveryGuaranteeDescription"),
                        def.getConfigDefinitions().get(1).getDescription());
    Assert.assertEquals(expected.get("badRecordsHandlingLabel"), def.getConfigDefinitions().get(2).getLabel());
    Assert.assertEquals(expected.get("badRecordsHandlingDescription"),
                        def.getConfigDefinitions().get(2).getDescription());
    Assert.assertEquals(expected.get("clusterSlaveMemoryLabel"), def.getConfigDefinitions().get(6).getLabel());
    Assert.assertEquals(expected.get("clusterSlaveMemoryDescription"),
                        def.getConfigDefinitions().get(6).getDescription());
    Assert.assertEquals(expected.get("clusterLauncherEnvLabel"), def.getConfigDefinitions().get(7).getLabel());
    Assert.assertEquals(expected.get("clusterLauncherEnvDescription"),
                        def.getConfigDefinitions().get(7).getDescription());

  }

  private static final Map<String, String> EXPECTED_BUILT_IN = new HashMap<>();

  static {
    EXPECTED_BUILT_IN.put("pipelineConstantsGroup", PipelineDefConfigs.Groups.CONSTANTS.getLabel());
    EXPECTED_BUILT_IN.put("pipelineErrorRecordsGroup", PipelineDefConfigs.Groups.BAD_RECORDS.getLabel());
    EXPECTED_BUILT_IN.put("pipelineClusterGroup", PipelineDefConfigs.Groups.CLUSTER.getLabel());
    EXPECTED_BUILT_IN.put("deliveryGuaranteeLabel", "Delivery Guarantee");
    EXPECTED_BUILT_IN.put("deliveryGuaranteeDescription", "");
    EXPECTED_BUILT_IN.put("badRecordsHandlingLabel", "Error Records");
    EXPECTED_BUILT_IN.put("badRecordsHandlingDescription", "");
    EXPECTED_BUILT_IN.put("executionModeLabel", PipelineDefConfigs.EXECUTION_MODE_LABEL);
    EXPECTED_BUILT_IN.put("executionModeDescription", PipelineDefConfigs.EXECUTION_MODE_DESCRIPTION);
    EXPECTED_BUILT_IN.put("clusterSlaveMemoryLabel", PipelineDefConfigs.CLUSTER_SLAVE_MEMORY_LABEL);
    EXPECTED_BUILT_IN.put("clusterSlaveMemoryDescription", PipelineDefConfigs.CLUSTER_SLAVE_MEMORY_DESCRIPTION);
    EXPECTED_BUILT_IN.put("clusterLauncherEnvLabel", PipelineDefConfigs.CLUSTER_LAUNCHER_ENV_LABEL);
    EXPECTED_BUILT_IN.put("clusterLauncherEnvDescription", PipelineDefConfigs.CLUSTER_LAUNCHER_ENV_DESCRIPTION);
  }

  @Test
  public void testBuiltInMessages() throws Exception {
    testMessages(Locale.getDefault(), EXPECTED_BUILT_IN);
  }

  private static final Map<String, String> EXPECTED_RESOURCE_BUNDLE = new HashMap<>();

  static {
    EXPECTED_RESOURCE_BUNDLE.put("pipelineConstantsGroup", "XConstants");
    EXPECTED_RESOURCE_BUNDLE.put("pipelineErrorRecordsGroup", "XError Records");
    EXPECTED_RESOURCE_BUNDLE.put("pipelineClusterGroup", "XCluster");
    EXPECTED_RESOURCE_BUNDLE.put("deliveryGuaranteeLabel", "XDelivery Guarantee");
    EXPECTED_RESOURCE_BUNDLE.put("deliveryGuaranteeDescription", "X");
    EXPECTED_RESOURCE_BUNDLE.put("badRecordsHandlingLabel", "XError Records");
    EXPECTED_RESOURCE_BUNDLE.put("badRecordsHandlingDescription", "X");
    EXPECTED_RESOURCE_BUNDLE.put("executionModeLabel", "XExecution Mode");
    EXPECTED_RESOURCE_BUNDLE.put("executionModeDescription", "X");
    EXPECTED_RESOURCE_BUNDLE.put("clusterSlaveMemoryLabel", "XSlave Heap (MB)");
    EXPECTED_RESOURCE_BUNDLE.put("clusterSlaveMemoryDescription", "X");
    EXPECTED_RESOURCE_BUNDLE.put("clusterLauncherEnvLabel", "XLauncher ENV");
    EXPECTED_RESOURCE_BUNDLE.put("clusterLauncherEnvDescription", "X");
  }

  @Test
  public void testResourceBundleMessages() throws Exception {
    testMessages(new Locale("xyz"), EXPECTED_RESOURCE_BUNDLE);
  }

}
