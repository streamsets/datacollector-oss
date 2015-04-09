/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.alerts.AlertManager;
import com.streamsets.pipeline.alerts.AlertsUtil;
import com.streamsets.pipeline.alerts.TestDataRuleEvaluator;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.config.MetricElement;
import com.streamsets.pipeline.config.MetricType;
import com.streamsets.pipeline.config.MetricsRuleDefinition;
import com.streamsets.pipeline.config.RuleDefinitions;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TestMetricObserverRunner {

  private static final String LANE = "lane";
  private static final String ID = "myId";
  private static final String PIPELINE_NAME = "myPipeline";
  private static final String REVISION = "1.0";
  private MetricsObserverRunner metricObserverRunner;
  private MetricRegistry metrics = new MetricRegistry();
  private static RuntimeInfo runtimeInfo;

  @Before
  public void setUp() {
    runtimeInfo = new RuntimeInfo(new MetricRegistry(), Arrays.asList(TestDataRuleEvaluator.class.getClassLoader()));
    metricObserverRunner = new MetricsObserverRunner(metrics, new AlertManager(PIPELINE_NAME, REVISION, null, metrics,
      runtimeInfo, null));
  }

  @Test
  public void testMetricObserverRunner() {
    Timer t = MetricsConfigurator.createTimer(metrics, "testTimerMatch");
    t.update(1000, TimeUnit.MILLISECONDS);
    t.update(2000, TimeUnit.MILLISECONDS);
    t.update(3000, TimeUnit.MILLISECONDS);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testTimerMatch", "testTimerMatch",
      "testTimerMatch", MetricType.TIMER,
      MetricElement.TIMER_COUNT, "${value()>2}", false, true);

    List<MetricsRuleDefinition> metricsRuleDefinitions = new ArrayList<>();
    metricsRuleDefinitions.add(metricsRuleDefinition);
    RuleDefinitions ruleDefinitions = new RuleDefinitions(metricsRuleDefinitions,
      Collections.<DataRuleDefinition>emptyList(), Collections.<String>emptyList(), UUID.randomUUID());
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest =
      new RulesConfigurationChangeRequest(ruleDefinitions, Collections.<String>emptySet(),
        Collections.<String>emptySet(), null, null);

    metricObserverRunner.setRulesConfigurationChangeRequest(rulesConfigurationChangeRequest);
    metricObserverRunner.evaluate();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long)3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));

    //modify metric alert and add it to the list of alerts to be removed
    ruleDefinitions = new RuleDefinitions(Collections.<MetricsRuleDefinition>emptyList(),
      Collections.<DataRuleDefinition>emptyList(), Collections.<String>emptyList(), UUID.randomUUID());
    rulesConfigurationChangeRequest =
      new RulesConfigurationChangeRequest(ruleDefinitions, Collections.<String>emptySet(),
        ImmutableSet.of(metricsRuleDefinition.getId()), null, null);

    metricObserverRunner.setRulesConfigurationChangeRequest(rulesConfigurationChangeRequest);
    metricObserverRunner.evaluate();

    gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNull(gauge);

  }

}
