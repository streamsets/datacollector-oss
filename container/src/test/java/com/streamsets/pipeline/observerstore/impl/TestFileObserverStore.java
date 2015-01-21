/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.observerstore.impl;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.config.AlertDefinition;
import com.streamsets.pipeline.config.CounterDefinition;
import com.streamsets.pipeline.config.MetricElement;
import com.streamsets.pipeline.config.MetricType;
import com.streamsets.pipeline.config.MetricsAlertDefinition;
import com.streamsets.pipeline.config.SamplingDefinition;
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
    Assert.assertTrue(observerStore.retrieveAlerts(PIPELINE_NAME, PIPELINE_REV).isEmpty());

    List<AlertDefinition> alerts = new ArrayList<>();
    alerts.add(new AlertDefinition("a1", "a1", "a", "2", null, true));
    alerts.add(new AlertDefinition("a2", "a1", "a", "2", null, true));
    alerts.add(new AlertDefinition("a3", "a1", "a", "2", null, true));

    observerStore.storeAlerts(PIPELINE_NAME, PIPELINE_REV, alerts);

    List<AlertDefinition> actualAlerts = observerStore.retrieveAlerts(PIPELINE_NAME, PIPELINE_REV);

    Assert.assertEquals(alerts.size(), actualAlerts.size());
    for(int i = 0; i < alerts.size(); i++) {
      Assert.assertEquals(alerts.get(i).toString(), actualAlerts.get(i).toString());
    }

  }

  @Test
  public void testStoreAndRetrieveMetricAlerts() {
    Assert.assertTrue(observerStore.retrieveMetricAlerts(PIPELINE_NAME, PIPELINE_REV).isEmpty());

    List<MetricsAlertDefinition> metricsAlertDefinitions = new ArrayList<>();
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m1", "m1", "a", MetricType.COUNTER,
      MetricElement.COUNTER_COUNT, "p", true));
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m2", "m2", "a", MetricType.TIMER,
      MetricElement.TIMER_M15_RATE, "p", true));
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m3", "m3", "a", MetricType.HISTOGRAM,
      MetricElement.HISTOGRAM_MEAN, "p", true));

    observerStore.storeMetricAlerts(PIPELINE_NAME, PIPELINE_REV, metricsAlertDefinitions);

    List<MetricsAlertDefinition> actualMetricAlerts = observerStore.retrieveMetricAlerts(PIPELINE_NAME, PIPELINE_REV);

    Assert.assertEquals(metricsAlertDefinitions.size(), actualMetricAlerts.size());
    for(int i = 0; i < metricsAlertDefinitions.size(); i++) {
      Assert.assertEquals(metricsAlertDefinitions.get(i).toString(), actualMetricAlerts.get(i).toString());
    }

  }


  @Test
  public void testStoreAndRetrieveSamplingDefinitions() {
    Assert.assertTrue(observerStore.retrieveSamplingDefinitions(PIPELINE_NAME, PIPELINE_REV).isEmpty());

    List<SamplingDefinition> samplingDefinitions = new ArrayList<>();
    samplingDefinitions.add(new SamplingDefinition("s1", "s1", "a", "2", null, true));
    samplingDefinitions.add(new SamplingDefinition("s2", "s2", "a", "2", null, true));
    samplingDefinitions.add(new SamplingDefinition("s3", "s3", "a", "2", null, true));

    observerStore.storeSamplingDefinitions(PIPELINE_NAME, PIPELINE_REV, samplingDefinitions);

    List<SamplingDefinition> actualSamplingDefinitions =
      observerStore.retrieveSamplingDefinitions(PIPELINE_NAME, PIPELINE_REV);

    Assert.assertEquals(samplingDefinitions.size(), actualSamplingDefinitions.size());
    for(int i = 0; i < samplingDefinitions.size(); i++) {
      Assert.assertEquals(samplingDefinitions.get(i).toString(), actualSamplingDefinitions.get(i).toString());
    }

  }

  @Test
  public void testStoreAndRetrieveCounters() {
    Assert.assertTrue(observerStore.retrieveCounters(PIPELINE_NAME, PIPELINE_REV).isEmpty());

    List<CounterDefinition> counters = new ArrayList<>();
    counters.add(new CounterDefinition("c1", "c1", "l", "p", "g", true, null));
    counters.add(new CounterDefinition("c2", "c2", "l", "p", "g", true, null));
    counters.add(new CounterDefinition("c3", "c3", "l", "p", "g", true, null));

    observerStore.storeCounters(PIPELINE_NAME, PIPELINE_REV, counters);

    List<CounterDefinition> actualCounters = observerStore.retrieveCounters(PIPELINE_NAME, PIPELINE_REV);

    Assert.assertEquals(counters.size(), actualCounters.size());
    for(int i = 0; i < counters.size(); i++) {
      Assert.assertEquals(counters.get(i).toString(), actualCounters.get(i).toString());
    }

  }

}
