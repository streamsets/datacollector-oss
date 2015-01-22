/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.observerstore.impl;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.config.AlertDefinition;
import com.streamsets.pipeline.config.MeterDefinition;
import com.streamsets.pipeline.config.MetricElement;
import com.streamsets.pipeline.config.MetricType;
import com.streamsets.pipeline.config.MetricsAlertDefinition;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.config.SamplingDefinition;
import com.streamsets.pipeline.config.ThresholdType;
import com.streamsets.pipeline.main.RuntimeInfo;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestFileObserverStore {

  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_REV = "0";

  private FileObserverStore observerStore = null;

  @BeforeClass
  public static void beforeClass() {
    System.setProperty(RuntimeInfo.DATA_DIR, "./target/var");
  }

  @AfterClass
  public static void afterClass() {
    System.getProperties().remove(RuntimeInfo.DATA_DIR);
  }

  @Before
  public void setUp() throws IOException {
    File f = new File(System.getProperty(RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    RuntimeInfo info = new RuntimeInfo(ImmutableList.of(getClass().getClassLoader()));
    observerStore = new FileObserverStore(info);
  }

  @After
  public void tearDown() {

  }

  @Test
  public void testStoreAndRetrieveAlerts() {
    RuleDefinition ruleDefinition = observerStore.retrieveRules(PIPELINE_NAME, PIPELINE_REV);
    Assert.assertTrue(ruleDefinition.getAlertDefinitions().isEmpty());
    Assert.assertTrue(ruleDefinition.getMeterDefinitions().isEmpty());
    Assert.assertTrue(ruleDefinition.getSamplingDefinitions().isEmpty());
    Assert.assertTrue(ruleDefinition.getMetricsAlertDefinitions().isEmpty());


    List<AlertDefinition> alerts = new ArrayList<>();
    alerts.add(new AlertDefinition("a1", "a1", "a", "2", ThresholdType.COUNT, "10", 100, true));
    alerts.add(new AlertDefinition("a2", "a1", "a", "2", ThresholdType.COUNT, "10", 100, true));
    alerts.add(new AlertDefinition("a3", "a1", "a", "2", ThresholdType.COUNT, "10", 100, true));

    List<MetricsAlertDefinition> metricsAlertDefinitions = new ArrayList<>();
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m1", "m1", "a", MetricType.COUNTER,
      MetricElement.COUNTER_COUNT, "p", true));
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m2", "m2", "a", MetricType.TIMER,
      MetricElement.TIMER_M15_RATE, "p", true));
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m3", "m3", "a", MetricType.HISTOGRAM,
      MetricElement.HISTOGRAM_MEAN, "p", true));

    List<SamplingDefinition> samplingDefinitions = new ArrayList<>();
    samplingDefinitions.add(new SamplingDefinition("s1", "s1", "a", "2", null, true));
    samplingDefinitions.add(new SamplingDefinition("s2", "s2", "a", "2", null, true));
    samplingDefinitions.add(new SamplingDefinition("s3", "s3", "a", "2", null, true));

    List<MeterDefinition> counters = new ArrayList<>();
    counters.add(new MeterDefinition("c1", "c1", "l", "p", "g", true));
    counters.add(new MeterDefinition("c2", "c2", "l", "p", "g", true));
    counters.add(new MeterDefinition("c3", "c3", "l", "p", "g", true));

    ruleDefinition = new RuleDefinition(alerts, metricsAlertDefinitions, samplingDefinitions, counters);

    observerStore.storeRules(PIPELINE_NAME, PIPELINE_REV, ruleDefinition);

    RuleDefinition actualRuleDefinition = observerStore.retrieveRules(PIPELINE_NAME, PIPELINE_REV);

    Assert.assertEquals(ruleDefinition.getAlertDefinitions().size(), actualRuleDefinition.getAlertDefinitions().size());
    for(int i = 0; i < alerts.size(); i++) {
      Assert.assertEquals(ruleDefinition.getAlertDefinitions().get(i).toString(), actualRuleDefinition.getAlertDefinitions().get(i).toString());
    }

    Assert.assertEquals(ruleDefinition.getMetricsAlertDefinitions().size(), actualRuleDefinition.getMetricsAlertDefinitions().size());
    for(int i = 0; i < alerts.size(); i++) {
      Assert.assertEquals(ruleDefinition.getMetricsAlertDefinitions().get(i).toString(), actualRuleDefinition.getMetricsAlertDefinitions().get(i).toString());
    }
    Assert.assertEquals(ruleDefinition.getSamplingDefinitions().size(), actualRuleDefinition.getSamplingDefinitions().size());
    for(int i = 0; i < alerts.size(); i++) {
      Assert.assertEquals(ruleDefinition.getSamplingDefinitions().get(i).toString(), actualRuleDefinition.getSamplingDefinitions().get(i).toString());
    }
    Assert.assertEquals(ruleDefinition.getMeterDefinitions().size(), actualRuleDefinition.getMeterDefinitions().size());
    for(int i = 0; i < alerts.size(); i++) {
      Assert.assertEquals(ruleDefinition.getMeterDefinitions().get(i).toString(), actualRuleDefinition.getMeterDefinitions().get(i).toString());
    }

  }

}
