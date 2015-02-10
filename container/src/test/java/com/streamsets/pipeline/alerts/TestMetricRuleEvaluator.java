/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.alerts;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.streamsets.pipeline.config.MetricElement;
import com.streamsets.pipeline.config.MetricType;
import com.streamsets.pipeline.config.MetricsRuleDefinition;
import com.streamsets.pipeline.el.ELBasicSupport;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.el.ELStringSupport;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestMetricRuleEvaluator {

  private static final String LANE = "lane";
  private static final String PIPELINE_NAME = "myPipeline";
  private static final String REVISION = "1.0";

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
  public void testTimerMatch() {
    //create timer with id "testMetricAlerts" and register with metric registry, bump up value to 4.
    Timer t = MetricsConfigurator.createTimer(metrics, "testTimerMatch");
    t.update(1000, TimeUnit.MILLISECONDS);
    t.update(2000, TimeUnit.MILLISECONDS);
    t.update(3000, TimeUnit.MILLISECONDS);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testTimerMatch", "testTimerMatch",
      "testTimerMatch.timer", MetricType.TIMER,
      MetricElement.TIMER_COUNT, "${value()>2}", false, true);
    MetricRuleEvaluator metricRuleEvaluator = new MetricRuleEvaluator(metricsRuleDefinition, metrics, variables,
      elEvaluator, new AlertManager(PIPELINE_NAME, REVISION, null, metrics), Collections.<String>emptyList());
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
    Timer t = MetricsConfigurator.createTimer(metrics, "testTimerMatchDisabled");
    t.update(1000, TimeUnit.MILLISECONDS);
    t.update(2000, TimeUnit.MILLISECONDS);
    t.update(3000, TimeUnit.MILLISECONDS);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testTimerMatchDisabled",
      "testTimerMatchDisabled", "testTimerMatchDisabled.timer", MetricType.TIMER, MetricElement.TIMER_COUNT,
      "${value()>2}", false, false);
    MetricRuleEvaluator metricRuleEvaluator = new MetricRuleEvaluator(metricsRuleDefinition, metrics, variables,
      elEvaluator, new AlertManager(PIPELINE_NAME, REVISION, null, metrics), Collections.<String>emptyList());
    metricRuleEvaluator.checkForAlerts();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testTimerNoMatch() {
    //create timer with id "testMetricAlerts" and register with metric registry, bump up value to 4.
    Timer t = MetricsConfigurator.createTimer(metrics, "testTimerNoMatch");
    t.update(1000, TimeUnit.MILLISECONDS);
    t.update(2000, TimeUnit.MILLISECONDS);
    t.update(3000, TimeUnit.MILLISECONDS);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testTimerNoMatch", "testTimerNoMatch",
      "testTimerNoMatch.timer", MetricType.TIMER,
      MetricElement.TIMER_COUNT, "${value()>4}", false, true);
    MetricRuleEvaluator metricRuleEvaluator = new MetricRuleEvaluator(metricsRuleDefinition, metrics, variables,
      elEvaluator, new AlertManager(PIPELINE_NAME, REVISION, null, metrics), Collections.<String>emptyList());
    metricRuleEvaluator.checkForAlerts();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }
}
