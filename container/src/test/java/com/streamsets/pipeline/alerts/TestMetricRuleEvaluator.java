/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.alerts;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.streamsets.pipeline.config.MetricElement;
import com.streamsets.pipeline.config.MetricType;
import com.streamsets.pipeline.config.MetricsRuleDefinition;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELVariables;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringEL;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestMetricRuleEvaluator {

  private static final String LANE = "lane";
  private static final String PIPELINE_NAME = "myPipeline";
  private static final String REVISION = "1.0";

  private static MetricRegistry metrics;
  private static ELEvaluator elEvaluator;
  private static ELVariables variables;
  private static RuntimeInfo runtimeInfo;

  @BeforeClass
  public static void setUp() {
    metrics = new MetricRegistry();
    variables = new ELVariables();
    elEvaluator = new ELEvaluator("TestMetricRuleEvaluator", RecordEL.class, StringEL.class);
    runtimeInfo = new RuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
      Arrays.asList(TestDataRuleEvaluator.class.getClassLoader()));
  }

  @Test
  public void testTimerMatch() {
    //create timer with id "testMetricAlerts" and register with metric registry, bump up value to 4.
    Timer t = MetricsConfigurator.createTimer(metrics, "testTimerMatch", PIPELINE_NAME, REVISION);
    t.update(1000, TimeUnit.MILLISECONDS);
    t.update(2000, TimeUnit.MILLISECONDS);
    t.update(3000, TimeUnit.MILLISECONDS);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testTimerMatch", "testTimerMatch",
      "testTimerMatch", MetricType.TIMER,
      MetricElement.TIMER_COUNT, "${value()>2}", false, true);
    MetricRuleEvaluator metricRuleEvaluator = new MetricRuleEvaluator(metricsRuleDefinition, metrics,
      new AlertManager(PIPELINE_NAME, REVISION, null, metrics, runtimeInfo, null, null), Collections.<String>emptyList());
    metricRuleEvaluator.checkForAlerts();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long)3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
  }

  @Test
  public void testTimerMatchDisabled() {
    //create timer with id "testMetricAlerts" and register with metric registry, bump up value to 4.
    Timer t = MetricsConfigurator.createTimer(metrics, "testTimerMatchDisabled", PIPELINE_NAME, REVISION);
    t.update(1000, TimeUnit.MILLISECONDS);
    t.update(2000, TimeUnit.MILLISECONDS);
    t.update(3000, TimeUnit.MILLISECONDS);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testTimerMatchDisabled",
      "testTimerMatchDisabled", "testTimerMatchDisabled", MetricType.TIMER, MetricElement.TIMER_COUNT,
      "${value()>2}", false, false);
    MetricRuleEvaluator metricRuleEvaluator = new MetricRuleEvaluator(metricsRuleDefinition, metrics,
      new AlertManager(PIPELINE_NAME, REVISION, null, metrics, runtimeInfo, null, null), Collections.<String>emptyList());
    metricRuleEvaluator.checkForAlerts();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testTimerNoMatch() {
    //create timer with id "testMetricAlerts" and register with metric registry, bump up value to 4.
    Timer t = MetricsConfigurator.createTimer(metrics, "testTimerNoMatch", PIPELINE_NAME, REVISION);
    t.update(1000, TimeUnit.MILLISECONDS);
    t.update(2000, TimeUnit.MILLISECONDS);
    t.update(3000, TimeUnit.MILLISECONDS);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testTimerNoMatch", "testTimerNoMatch",
      "testTimerNoMatch", MetricType.TIMER,
      MetricElement.TIMER_COUNT, "${value()>4}", false, true);
    MetricRuleEvaluator metricRuleEvaluator = new MetricRuleEvaluator(metricsRuleDefinition, metrics,
      new AlertManager(PIPELINE_NAME, REVISION, null, metrics, runtimeInfo, null, null), Collections.<String>emptyList());
    metricRuleEvaluator.checkForAlerts();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testSoftErrorOnWrongCondition() {
    //create timer with id "testMetricAlerts" and register with metric registry, bump up value to 4.
    Timer t = MetricsConfigurator.createTimer(metrics, "testSoftErrorOnWrongCondition", PIPELINE_NAME, REVISION);
    t.update(1000, TimeUnit.MILLISECONDS);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testSoftErrorOnWrongCondition",
      "testSoftErrorOnWrongCondition", "testSoftErrorOnWrongCondition", MetricType.TIMER,
      //invalid condition
      MetricElement.TIMER_COUNT, "${valu()>2", false, true);
    MetricRuleEvaluator metricRuleEvaluator = new MetricRuleEvaluator(metricsRuleDefinition, metrics,
      new AlertManager(PIPELINE_NAME, REVISION, null, metrics, runtimeInfo, null, null), Collections.<String>emptyList());
    metricRuleEvaluator.checkForAlerts();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("exceptionMessage"));
  }

  @Test
  public void testCounterMatch() {
    //create timer with id "testMetricAlerts" and register with metric registry, bump up value to 4.
    Counter c = MetricsConfigurator.createCounter(metrics, "testCounterMatch", PIPELINE_NAME, REVISION);
    c.inc(100);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testCounterMatch", "testCounterMatch",
      "testCounterMatch", MetricType.COUNTER,
      MetricElement.COUNTER_COUNT, "${value()>98}", false, true);
    MetricRuleEvaluator metricRuleEvaluator = new MetricRuleEvaluator(metricsRuleDefinition, metrics,
      new AlertManager(PIPELINE_NAME, REVISION, null, metrics, runtimeInfo, null, null), Collections.<String>emptyList());
    metricRuleEvaluator.checkForAlerts();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long)100, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
  }

  @Test
  public void testCounterDisabled() {
    //create timer with id "testMetricAlerts" and register with metric registry, bump up value to 4.
    Counter c = MetricsConfigurator.createCounter(metrics, "testCounterDisabled", PIPELINE_NAME, REVISION);
    c.inc(100);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testCounterDisabled",
      "testCounterDisabled", "testCounterDisabled", MetricType.COUNTER,
      MetricElement.COUNTER_COUNT, "${value()>98}", false, false);
    MetricRuleEvaluator metricRuleEvaluator = new MetricRuleEvaluator(metricsRuleDefinition, metrics,
      new AlertManager(PIPELINE_NAME, REVISION, null, metrics, runtimeInfo, null, null), Collections.<String>emptyList());
    metricRuleEvaluator.checkForAlerts();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testCounterNoMatch() {
    //create timer with id "testMetricAlerts" and register with metric registry, bump up value to 4.
    Counter c = MetricsConfigurator.createCounter(metrics, "testCounterNoMatch", PIPELINE_NAME, REVISION);
    c.inc(100);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testCounterNoMatch",
      "testCounterNoMatch", "testCounterNoMatch", MetricType.COUNTER,
      MetricElement.COUNTER_COUNT, "${value()>100}", false, true);
    MetricRuleEvaluator metricRuleEvaluator = new MetricRuleEvaluator(metricsRuleDefinition, metrics,
      new AlertManager(PIPELINE_NAME, REVISION, null, metrics, runtimeInfo, null, null), Collections.<String>emptyList());
    metricRuleEvaluator.checkForAlerts();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testMeterMatch() {
    //create timer with id "testMetricAlerts" and register with metric registry, bump up value to 4.
    Meter m = MetricsConfigurator.createMeter(metrics, "testMeterMatch", PIPELINE_NAME, REVISION);
    m.mark(1000);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testMeterMatch", "testMeterMatch",
      "testMeterMatch", MetricType.METER,
      MetricElement.METER_COUNT, "${value()>98}", false, true);
    MetricRuleEvaluator metricRuleEvaluator = new MetricRuleEvaluator(metricsRuleDefinition, metrics,
      new AlertManager(PIPELINE_NAME, REVISION, null, metrics, runtimeInfo, null, null), Collections.<String>emptyList());
    metricRuleEvaluator.checkForAlerts();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long)1000, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
  }

  @Test
  public void testMeterNoMatch() {
    //create timer with id "testMetricAlerts" and register with metric registry, bump up value to 4.
    Meter m = MetricsConfigurator.createMeter(metrics, "testMeterNoMatch", PIPELINE_NAME, REVISION);
    m.mark(1000);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testMeterNoMatch", "testMeterNoMatch",
      "testMeterNoMatch", MetricType.METER,
      MetricElement.METER_COUNT, "${value()>1001}", false, true);
    MetricRuleEvaluator metricRuleEvaluator = new MetricRuleEvaluator(metricsRuleDefinition, metrics,
      new AlertManager(PIPELINE_NAME, REVISION, null, metrics, runtimeInfo, null, null), Collections.<String>emptyList());
    metricRuleEvaluator.checkForAlerts();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testMeterDisabled() {
    //create timer with id "testMetricAlerts" and register with metric registry, bump up value to 4.
    Meter m = MetricsConfigurator.createMeter(metrics, "testMeterDisabled", PIPELINE_NAME, REVISION);
    m.mark(1000);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testMeterDisabled", "testMeterDisabled",
      "testMeterDisabled", MetricType.METER,
      MetricElement.METER_COUNT, "${value()>100}", false, false);
    MetricRuleEvaluator metricRuleEvaluator = new MetricRuleEvaluator(metricsRuleDefinition, metrics,
      new AlertManager(PIPELINE_NAME, REVISION, null, metrics, runtimeInfo, null, null), Collections.<String>emptyList());
    metricRuleEvaluator.checkForAlerts();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testHistogramMatch() {
    //create timer with id "testMetricAlerts" and register with metric registry, bump up value to 4.
    Histogram h = MetricsConfigurator.createHistogram5Min(metrics, "testHistogramMatch", PIPELINE_NAME, REVISION);
    h.update(1000);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testHistogramMatch", "testHistogramMatch",
      "testHistogramMatch", MetricType.HISTOGRAM,
      MetricElement.HISTOGRAM_COUNT, "${value()==1}", false, true);
    MetricRuleEvaluator metricRuleEvaluator = new MetricRuleEvaluator(metricsRuleDefinition, metrics,
      new AlertManager(PIPELINE_NAME, REVISION, null, metrics, runtimeInfo, null, null), Collections.<String>emptyList());
    metricRuleEvaluator.checkForAlerts();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long)1, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
  }

  @Test
  public void testHistogramNoMatch() {
    //create timer with id "testMetricAlerts" and register with metric registry, bump up value to 4.
    Histogram h = MetricsConfigurator.createHistogram5Min(metrics, "testHistogramNoMatch", PIPELINE_NAME, REVISION);
    h.update(1000);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testHistogramNoMatch",
      "testHistogramNoMatch", "testHistogramNoMatch", MetricType.HISTOGRAM,
      MetricElement.HISTOGRAM_COUNT, "${value()>1}", false, true);
    MetricRuleEvaluator metricRuleEvaluator = new MetricRuleEvaluator(metricsRuleDefinition, metrics,
      new AlertManager(PIPELINE_NAME, REVISION, null, metrics, runtimeInfo, null, null), Collections.<String>emptyList());
    metricRuleEvaluator.checkForAlerts();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testHistogramDisabled() {
    //create timer with id "testMetricAlerts" and register with metric registry, bump up value to 4.
    Histogram h = MetricsConfigurator.createHistogram5Min(metrics, "testHistogramDisabled", PIPELINE_NAME, REVISION);
    h.update(1000);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testHistogramDisabled",
      "testHistogramDisabled", "testHistogramDisabled", MetricType.HISTOGRAM,
      MetricElement.HISTOGRAM_COUNT, "${value()==1}", false, false);
    MetricRuleEvaluator metricRuleEvaluator = new MetricRuleEvaluator(metricsRuleDefinition, metrics,
      new AlertManager(PIPELINE_NAME, REVISION, null, metrics, runtimeInfo, null, null), Collections.<String>emptyList());
    metricRuleEvaluator.checkForAlerts();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }
}
