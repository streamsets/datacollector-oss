/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.alerts;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.config.AlertDefinition;
import com.streamsets.pipeline.config.ThresholdType;
import com.streamsets.pipeline.el.ELBasicSupport;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.el.ELStringSupport;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class TestAlertsChecker {

  private static final String LANE = "lane";

  private static MetricRegistry metrics;
  private static ELEvaluator elEvaluator;
  private static ELEvaluator.Variables variables;

  @BeforeClass
  public static void setUp() {
    metrics = new MetricRegistry();
    variables = new ELEvaluator.Variables();
    elEvaluator = new ELEvaluator();
    ELBasicSupport.registerBasicFunctions(elEvaluator);
    ELRecordSupport.registerRecordFunctions(elEvaluator);
    ELStringSupport.registerStringFunctions(elEvaluator);
  }

  @Test
  public void testAlertRaisedCount() {
    AlertDefinition alertDefinition = new AlertDefinition("testAlertRaisedCount", "testAlertRaisedCount", "testAlertRaisedCount",
      "${record:value(\"/name\")==null}", ThresholdType.COUNT, "2", 5, true);
    AlertsChecker alertsChecker = new AlertsChecker(alertDefinition, metrics, variables, elEvaluator);
    alertsChecker.checkForAlerts(TestUtil.createSnapshot("testAlertRaisedCount"));
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGuageName(alertDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));
  }

  @Test
  public void testAlertRaisedCountDisabled() {
    AlertDefinition alertDefinition = new AlertDefinition("testAlertRaisedCountDisabled", "testAlertRaisedCountDisabled", "testAlertRaisedCountDisabled",
      "${record:value(\"/name\")==null}", ThresholdType.COUNT, "2", 5, false);
    AlertsChecker alertsChecker = new AlertsChecker(alertDefinition, metrics, variables, elEvaluator);
    alertsChecker.checkForAlerts(TestUtil.createSnapshot("testAlertRaisedCountDisabled"));
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGuageName(alertDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testNoExceptionInvalidExpression() {
    //Missing "}"
    AlertDefinition alertDefinition = new AlertDefinition("testNoExceptionInvalidExpression", "testNoExceptionInvalidExpression", "testNoExceptionInvalidExpression",
      "${record:value(\"/name\")==null", ThresholdType.COUNT, "2", 5, true);
    AlertsChecker alertsChecker = new AlertsChecker(alertDefinition, metrics, variables, elEvaluator);
    alertsChecker.checkForAlerts(TestUtil.createSnapshot("testNoExceptionInvalidExpression"));
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGuageName(alertDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testNoAlertRaisedCount() {
    AlertDefinition alertDefinition = new AlertDefinition("testNoAlertRaisedCount", "testNoAlertRaisedCount", "testNoAlertRaisedCount",
      "${record:value(\"/name\")==null}", ThresholdType.COUNT, "3", 5, true);
    AlertsChecker alertsChecker = new AlertsChecker(alertDefinition, metrics, variables, elEvaluator);
    alertsChecker.checkForAlerts(TestUtil.createSnapshot("testNoAlertRaisedCount"));
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGuageName(alertDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testAlertRaisedPercentage() {
    AlertDefinition alertDefinition = new AlertDefinition("testAlertRaisedPercentage", "testAlertRaisedPercentage", "testAlertRaisedPercentage",
      "${record:value(\"/name\")==null}", ThresholdType.PERCENTAGE, "40", 5, true);
    AlertsChecker alertsChecker = new AlertsChecker(alertDefinition, metrics, variables, elEvaluator);
    alertsChecker.checkForAlerts(TestUtil.createSnapshot("testAlertRaisedPercentage"));
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGuageName(alertDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long)3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
  }

  @Test
  public void testNoAlertRaisedPercentage() {
    AlertDefinition alertDefinition = new AlertDefinition("testNoAlertRaisedPercentage", "testNoAlertRaisedPercentage", "testNoAlertRaisedPercentage",
      "${record:value(\"/name\")==null}", ThresholdType.PERCENTAGE, "60", 5, true);
    AlertsChecker alertsChecker = new AlertsChecker(alertDefinition, metrics, variables, elEvaluator);
    alertsChecker.checkForAlerts(TestUtil.createSnapshot("testNoAlertRaisedPercentage"));
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGuageName(alertDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testGaugeChange() {
    AlertDefinition alertDefinition = new AlertDefinition("testGaugeChange", "testGaugeChange", "testGaugeChange",
      "${record:value(\"/name\")==null}", ThresholdType.COUNT, "2", 5, true);
    AlertsChecker alertsChecker = new AlertsChecker(alertDefinition, metrics, variables, elEvaluator);

    alertsChecker.checkForAlerts(TestUtil.createSnapshot("testGaugeChange"));
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGuageName(alertDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));

    alertsChecker.checkForAlerts(TestUtil.createSnapshot("testGaugeChange"));
    gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGuageName(alertDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 6, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));
  }

}
