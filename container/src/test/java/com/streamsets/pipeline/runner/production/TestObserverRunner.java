/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.alerts.AlertManager;
import com.streamsets.pipeline.alerts.AlertsUtil;
import com.streamsets.pipeline.alerts.TestDataRuleEvaluator;
import com.streamsets.pipeline.alerts.TestUtil;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.config.RuleDefinitions;
import com.streamsets.pipeline.config.ThresholdType;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.util.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestObserverRunner {

  private static final String LANE = "lane";
  private static final String PIPELINE_NAME = "myPipeline";
  private static final String REVISION = "1.0";
  private ObserverRunner observerRunner;
  private MetricRegistry metrics = new MetricRegistry();
  private static RuntimeInfo runtimeInfo;

  @Before
  public void setUp() {
    runtimeInfo = new RuntimeInfo(Arrays.asList(TestDataRuleEvaluator.class.getClassLoader()));
    observerRunner = new ObserverRunner(metrics, new AlertManager(PIPELINE_NAME, REVISION, null, metrics, runtimeInfo),
      new Configuration());
  }

  @Test
  public void testHandleConfigurationChangeRequest() {
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest = createRulesConfigurationChangeRequest(false, false);
    observerRunner.handleConfigurationChangeRequest(rulesConfigurationChangeRequest);
    Assert.assertTrue(observerRunner.getRulesConfigurationChangeRequest() == rulesConfigurationChangeRequest);
  }

  @Test
  public void testHandleObserverRequestAlert() {
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest = createRulesConfigurationChangeRequest(true, false);
    observerRunner.handleConfigurationChangeRequest(rulesConfigurationChangeRequest);
    observerRunner.handleDataRulesEvaluationRequest(createProductionObserverRequest());
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGaugeName("myId"));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));
  }

  @Test
  public void testHandleObserverRequestMeter() {
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest = createRulesConfigurationChangeRequest(false, true);
    observerRunner.handleConfigurationChangeRequest(rulesConfigurationChangeRequest);
    observerRunner.handleDataRulesEvaluationRequest(createProductionObserverRequest());
    Meter meter = MetricsConfigurator.getMeter(metrics, AlertsUtil.getUserMeterName("myId"));
    Assert.assertNotNull(meter);
    Assert.assertEquals(3, meter.getCount());
  }

  @Test
  public void testHandleObserverRequestAlertAndMeter() {
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest = createRulesConfigurationChangeRequest(true, true);
    observerRunner.handleConfigurationChangeRequest(rulesConfigurationChangeRequest);
    observerRunner.handleDataRulesEvaluationRequest(createProductionObserverRequest());
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGaugeName("myId"));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));

    Meter meter = MetricsConfigurator.getMeter(metrics, AlertsUtil.getUserMeterName("myId"));
    Assert.assertNotNull(meter);
    Assert.assertEquals(3, meter.getCount());
  }

  private DataRulesEvaluationRequest createProductionObserverRequest() {
    DataRulesEvaluationRequest request = new DataRulesEvaluationRequest(TestUtil.createSnapshot(LANE),
      TestUtil.createLaneToRecordSizeMap(LANE));
    return request;
  }

  private RulesConfigurationChangeRequest createRulesConfigurationChangeRequest(boolean alert, boolean meter) {
    List<DataRuleDefinition> dataRuleDefinitions = new ArrayList<>();
    dataRuleDefinitions.add(new DataRuleDefinition("myId", "myRule", LANE + "::s", 100, 5,
      "${record:value(\"/name\")==null}", alert, "alertText", ThresholdType.COUNT, "2", 5, meter, false, true));
    RuleDefinitions ruleDefinitions = new RuleDefinitions(null, dataRuleDefinitions, Collections.<String>emptyList(),
      UUID.randomUUID());
    Map<String, List<DataRuleDefinition>> laneToRuleDefinition = new HashMap<>();
    laneToRuleDefinition.put(LANE + "::s", dataRuleDefinitions);
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest =
      new RulesConfigurationChangeRequest(ruleDefinitions, Collections.<String>emptySet(),
        Collections.<String>emptySet(), laneToRuleDefinition);
    return rulesConfigurationChangeRequest;
  }

}
