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
package com.streamsets.datacollector.execution.runner.common;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.ThresholdType;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.alerts.AlertManager;
import com.streamsets.datacollector.execution.alerts.TestDataRuleEvaluator;
import com.streamsets.datacollector.execution.alerts.TestUtil;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.runner.production.DataRulesEvaluationRequest;
import com.streamsets.datacollector.runner.production.RulesConfigurationChangeRequest;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
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

@SuppressWarnings("unchecked")
public class TestDataObserverRunner {

  private static final String LANE = "lane";
  private static final String ID = "myId";
  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_TITLE = "myPipelineTitle";
  private static final String REVISION = "1.0";
  private DataObserverRunner dataObserverRunner;
  private final MetricRegistry metrics = new MetricRegistry();
  private static RuntimeInfo runtimeInfo;

  @Before
  public void setUp() {
    runtimeInfo = new StandaloneRuntimeInfo(
        RuntimeInfo.SDC_PRODUCT,
        RuntimeModule.SDC_PROPERTY_PREFIX,
        new MetricRegistry(),
        Arrays.asList(TestDataRuleEvaluator.class.getClassLoader())
    );
    dataObserverRunner = new DataObserverRunner(PIPELINE_NAME, REVISION, metrics,
      new AlertManager(PIPELINE_NAME, PIPELINE_TITLE, REVISION, null, metrics, runtimeInfo,
          new EventListenerManager()),
        new Configuration(), null);
  }

  @Test
  public void testHandleConfigurationChangeRequest() {
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest = createRulesConfigurationChangeRequest(false, false);
    dataObserverRunner.handleConfigurationChangeRequest(rulesConfigurationChangeRequest);
    Assert.assertTrue(dataObserverRunner.getRulesConfigurationChangeRequest() == rulesConfigurationChangeRequest);
  }

  @Test
  public void testHandleObserverRequestAlert() {
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest = createRulesConfigurationChangeRequest(true, false);
    dataObserverRunner.handleConfigurationChangeRequest(rulesConfigurationChangeRequest);
    dataObserverRunner.handleDataRulesEvaluationRequest(createProductionObserverRequest());
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGaugeName("myId"));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));
  }

  @Test
  public void testHandleObserverRequestMeter() {
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest = createRulesConfigurationChangeRequest(false, true);
    dataObserverRunner.handleConfigurationChangeRequest(rulesConfigurationChangeRequest);
    dataObserverRunner.handleDataRulesEvaluationRequest(createProductionObserverRequest());
    Meter meter = MetricsConfigurator.getMeter(metrics, AlertsUtil.getUserMetricName("myId"));
    Assert.assertNotNull(meter);
    Assert.assertEquals(3, meter.getCount());
  }

  @Test
  public void testHandleObserverRequestAlertAndMeter() {
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest = createRulesConfigurationChangeRequest(true, true);
    dataObserverRunner.handleConfigurationChangeRequest(rulesConfigurationChangeRequest);
    dataObserverRunner.handleDataRulesEvaluationRequest(createProductionObserverRequest());
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGaugeName("myId"));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));

    Meter meter = MetricsConfigurator.getMeter(metrics, AlertsUtil.getUserMetricName("myId"));
    Assert.assertNotNull(meter);
    Assert.assertEquals(3, meter.getCount());
  }

  private DataRulesEvaluationRequest createProductionObserverRequest() {
    DataRulesEvaluationRequest request = new DataRulesEvaluationRequest(TestUtil.createSnapshot(LANE, ID),
      TestUtil.createLaneToRecordSizeMap(LANE));
    return request;
  }

  private RulesConfigurationChangeRequest createRulesConfigurationChangeRequest(boolean alert, boolean meter) {
    List<DataRuleDefinition> dataRuleDefinitions = new ArrayList<>();
    dataRuleDefinitions.add(
        new DataRuleDefinition(
            ID,
            "myRule",
            LANE + "::s",
            100,
            5,
            "${record:value(\"/name\")==null}",
            alert,
            "alertText",
            ThresholdType.COUNT,
            "2",
            5,
            meter,
            false,
            true,
            System.currentTimeMillis()
        )
    );
    RuleDefinitions ruleDefinitions = new RuleDefinitions(
        PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
        RuleDefinitionsConfigBean.VERSION,
        null,
        dataRuleDefinitions,
        null,
        Collections.<String>emptyList(),
        UUID.randomUUID(),
        Collections.emptyList()
    );
    Map<String, List<DataRuleDefinition>> laneToRuleDefinition = new HashMap<>();
    Map<String, Integer> ruleIdToSampledRecordsSize = new HashMap<>();
    laneToRuleDefinition.put(LANE + "::s", dataRuleDefinitions);
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest =
      new RulesConfigurationChangeRequest(ruleDefinitions, new HashMap<String, String>(),
        Collections.<String>emptySet(), laneToRuleDefinition, ruleIdToSampledRecordsSize);
    return rulesConfigurationChangeRequest;
  }

  @Test
  public void testSampleRecords() {
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest = createRulesConfigurationChangeRequest(true, false);
    dataObserverRunner.handleConfigurationChangeRequest(rulesConfigurationChangeRequest);
    dataObserverRunner.handleDataRulesEvaluationRequest(createProductionObserverRequest());

    List<SampledRecord> sampleRecords = dataObserverRunner.getSampledRecords("myId", 5);
    Assert.assertNotNull(sampleRecords);
    Assert.assertEquals(5, sampleRecords.size());

    int matchedSampledRecords  = 0;
    for(SampledRecord sampledRecord: sampleRecords) {
      if(sampledRecord.isMatchedCondition()) {
        matchedSampledRecords++;
      }
    }
    Assert.assertEquals(3, matchedSampledRecords);

    Assert.assertTrue(rulesConfigurationChangeRequest.getRulesToRemove().isEmpty());
    //Create rule configuration change request where the previous rule is removed.
    // This simulates user action - change/disable/delete rule
    rulesConfigurationChangeRequest.getRulesToRemove().put("myId", "myLane");
    dataObserverRunner.handleConfigurationChangeRequest(rulesConfigurationChangeRequest);

    sampleRecords = dataObserverRunner.getSampledRecords("myId", 5);
    Assert.assertNotNull(sampleRecords);
    Assert.assertEquals(0, sampleRecords.size());
  }
}
