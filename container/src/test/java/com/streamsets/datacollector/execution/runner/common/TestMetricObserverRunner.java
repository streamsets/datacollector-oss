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
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.MetricElement;
import com.streamsets.datacollector.config.MetricType;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.alerts.AlertManager;
import com.streamsets.datacollector.execution.alerts.TestDataRuleEvaluator;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.runner.production.RulesConfigurationChangeRequest;

import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
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

public class
TestMetricObserverRunner {

  private static final String LANE = "lane";
  private static final String ID = "myId";
  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_TITLE = "myPipelineTitle";
  private static final String REVISION = "1.0";
  private MetricsObserverRunner metricObserverRunner;
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
    metricObserverRunner = new MetricsObserverRunner(
        PIPELINE_NAME,
        REVISION,
        false,
        metrics,
        new AlertManager(PIPELINE_NAME, PIPELINE_TITLE, REVISION, null, metrics, runtimeInfo, new EventListenerManager()),
        null,
        new Configuration(),
        runtimeInfo
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMetricObserverRunner() {
    Timer t = MetricsConfigurator.createTimer(metrics, "testTimerMatch", PIPELINE_NAME, REVISION);
    t.update(1000, TimeUnit.MILLISECONDS);
    t.update(2000, TimeUnit.MILLISECONDS);
    t.update(3000, TimeUnit.MILLISECONDS);

    MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition(
        "testTimerMatch",
        "testTimerMatch",
        "testTimerMatch",
        MetricType.TIMER,
        MetricElement.TIMER_COUNT,
        "${value()>2}",
        false,
        true,
        System.currentTimeMillis()
    );

    List<MetricsRuleDefinition> metricsRuleDefinitions = new ArrayList<>();
    metricsRuleDefinitions.add(metricsRuleDefinition);
    RuleDefinitions ruleDefinitions = new RuleDefinitions(
        PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
        RuleDefinitionsConfigBean.VERSION,
        metricsRuleDefinitions,
        Collections.<DataRuleDefinition>emptyList(),
        Collections.<DriftRuleDefinition>emptyList(),
        Collections.<String>emptyList(),
        UUID.randomUUID(),
        Collections.emptyList()
    );
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest =
      new RulesConfigurationChangeRequest(ruleDefinitions, Collections.<String, String>emptyMap(),
        Collections.<String>emptySet(), null, null);

    metricObserverRunner.setRulesConfigurationChangeRequest(rulesConfigurationChangeRequest);
    metricObserverRunner.evaluate();

    //get alert gauge
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long)3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));

    //modify metric alert and add it to the list of alerts to be removed
    ruleDefinitions = new RuleDefinitions(
        PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
        RuleDefinitionsConfigBean.VERSION,
        Collections.<MetricsRuleDefinition>emptyList(),
        Collections.<DataRuleDefinition>emptyList(),
        Collections.<DriftRuleDefinition>emptyList(),
        Collections.<String>emptyList(),
        UUID.randomUUID(),
        Collections.emptyList()
    );
    rulesConfigurationChangeRequest =
      new RulesConfigurationChangeRequest(ruleDefinitions, Collections.<String, String>emptyMap(),
        ImmutableSet.of(metricsRuleDefinition.getId()), null, null);

    metricObserverRunner.setRulesConfigurationChangeRequest(rulesConfigurationChangeRequest);
    metricObserverRunner.evaluate();

    gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(metricsRuleDefinition.getId()));
    Assert.assertNull(gauge);

  }

}
