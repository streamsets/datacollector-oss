/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.alerts.AlertsUtil;
import com.streamsets.pipeline.alerts.TestUtil;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.config.ThresholdType;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestObserverRunner {

  private static final String LANE = "lane";
  private ObserverRunner observerRunner;
  private MetricRegistry metrics = new MetricRegistry();

  @Before
  public void setUp() {
    observerRunner = new ObserverRunner(metrics, null);
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
    observerRunner.handleObserverRequest(createProductionObserverRequest());
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGaugeName("myId"));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));
  }

  @Test
  public void testHandleObserverRequestMeter() {
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest = createRulesConfigurationChangeRequest(false, true);
    observerRunner.handleConfigurationChangeRequest(rulesConfigurationChangeRequest);
    observerRunner.handleObserverRequest(createProductionObserverRequest());
    Meter meter = MetricsConfigurator.getMeter(metrics, AlertsUtil.getUserMeterName("myId"));
    Assert.assertNotNull(meter);
    Assert.assertEquals(3, meter.getCount());
  }

  @Test
  public void testHandleObserverRequestAlertAndMeter() {
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest = createRulesConfigurationChangeRequest(true, true);
    observerRunner.handleConfigurationChangeRequest(rulesConfigurationChangeRequest);
    observerRunner.handleObserverRequest(createProductionObserverRequest());
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGaugeName("myId"));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));

    Meter meter = MetricsConfigurator.getMeter(metrics, AlertsUtil.getUserMeterName("myId"));
    Assert.assertNotNull(meter);
    Assert.assertEquals(3, meter.getCount());
  }

  private ProductionObserveRequest createProductionObserverRequest() {
    ProductionObserveRequest request = new ProductionObserveRequest(TestUtil.createSnapshot(LANE));
    return request;
  }

  private RulesConfigurationChangeRequest createRulesConfigurationChangeRequest(boolean alert, boolean meter) {
    List<DataRuleDefinition> dataRuleDefinitions = new ArrayList<>();
    dataRuleDefinitions.add(new DataRuleDefinition("myId", "myRule", LANE + "::s", 100, 5,
      "${record:value(\"/name\")==null}", alert, ThresholdType.COUNT, "2", 5, meter, false, null, true));
    RuleDefinition ruleDefinition = new RuleDefinition(null, dataRuleDefinitions);
    Map<String, List<DataRuleDefinition>> laneToRuleDefinition = new HashMap<>();
    laneToRuleDefinition.put(LANE + "::s", dataRuleDefinitions);
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest =
      new RulesConfigurationChangeRequest(ruleDefinition, Collections.<String>emptySet(),
        Collections.<String>emptySet(), laneToRuleDefinition);
    return rulesConfigurationChangeRequest;
  }

}
