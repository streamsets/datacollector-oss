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
package com.streamsets.datacollector.execution.alerts;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.ThresholdType;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.definition.ConcreteELDefinitionExtractor;
import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.runner.common.SampledRecord;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.LaneResolver;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringEL;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class TestDataRuleEvaluator {

  private static final String USER_PREFIX = "user.";
  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_TITLE = "myPipelineTitle";
  private static final String REVISION = "1.0";

  private static MetricRegistry metrics;
  private static ELEvaluator elEvaluator;
  private static ELVariables variables;
  private static RuntimeInfo runtimeInfo;

  @BeforeClass
  public static void setUp() {
    metrics = new MetricRegistry();
    variables = new ELVariables();
    elEvaluator = new ELEvaluator("TestDataRuleEvaluator", ConcreteELDefinitionExtractor.get(), RecordEL.class, StringEL.class);
    runtimeInfo = new StandaloneRuntimeInfo(
        RuntimeInfo.SDC_PRODUCT,
        RuntimeModule.SDC_PROPERTY_PREFIX,
        new MetricRegistry(),
        Arrays.asList(TestDataRuleEvaluator.class.getClassLoader())
    );
  }

  @Test
  public void testAlertEnabledMeterEnabled() {
    String lane = "testAlertEnabledMeterEnabled";
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testAlertEnabledMeterEnabled",
      "testAlertEnabledMeterEnabled", lane, 100, 10, "${record:value(\"/name\")==null}", true,
      "testAlertEnabledMeterEnabled", ThresholdType.COUNT, "2", 5, true, false, true, System.currentTimeMillis());
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator("name", "0", metrics,
      new AlertManager(PIPELINE_NAME, PIPELINE_TITLE, REVISION, null, metrics, runtimeInfo, new EventListenerManager()),  new RuleDefinitionsConfigBean(),
        new HashMap<String, Object>(), dataRuleDefinition, new Configuration(), null, null);
    evaluateRule(dataRuleEvaluator, lane);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));

    Meter meter = MetricsConfigurator.getMeter(metrics, USER_PREFIX + dataRuleDefinition.getId());
    Assert.assertNotNull(meter);
    Assert.assertEquals(3, meter.getCount());
  }

  @Test
  public void testAlertDisabledMeterEnabled() {
    String lane = "testAlertDisabledMeterEnabled";
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testAlertDisabledMeterEnabled",
      "testAlertDisabledMeterEnabled", lane,
      100, 10, "${record:value(\"/name\")==null}", false, null, ThresholdType.COUNT, "2", 5, true,
      false, true, System.currentTimeMillis());
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator( "name", "0", metrics,
      new AlertManager(PIPELINE_NAME, PIPELINE_TITLE, REVISION, null, metrics, runtimeInfo, new EventListenerManager()),  new RuleDefinitionsConfigBean(),
        new HashMap<String, Object>(), dataRuleDefinition, new Configuration(), null, null);
    evaluateRule(dataRuleEvaluator, lane);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNull(gauge);

    Meter meter = MetricsConfigurator.getMeter(metrics, USER_PREFIX + dataRuleDefinition.getId());
    Assert.assertNotNull(meter);
    Assert.assertEquals(3, meter.getCount());
  }

  @Test
  public void testAlertEnabledMeterDisabled() {
    String lane = "testAlertEnabledMeterDisabled";
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testAlertEnabledMeterDisabled",
      "testAlertEnabledMeterDisabled", lane,
      100, 10, "${record:value(\"/name\")==null}", true, "testAlertEnabledMeterDisabled", ThresholdType.COUNT, "2",
      5, false, false, true, System.currentTimeMillis());
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator( "name", "0", metrics,
      new AlertManager(PIPELINE_NAME, PIPELINE_TITLE, REVISION, null, metrics, runtimeInfo, new EventListenerManager()), new RuleDefinitionsConfigBean(),
        new HashMap<String, Object>(), dataRuleDefinition, new Configuration(), null, null);
    evaluateRule(dataRuleEvaluator, lane);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));

    Meter meter = MetricsConfigurator.getMeter(metrics, USER_PREFIX + dataRuleDefinition.getId());
    Assert.assertNull(meter);
  }

  @Test
  public void testAlertRaisedCountRuleDisabled() {
    String lane = "testAlertRaisedCountRuleDisabled";
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testAlertRaisedCountRuleDisabled",
      "testAlertRaisedCountRuleDisabled", lane,
      100, 10, "${record:value(\"/name\")==null}", true, "testAlertRaisedCountRuleDisabled", ThresholdType.COUNT, "2",
      5, true, false, false, System.currentTimeMillis());
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator( "name", "0", metrics,
      new AlertManager(PIPELINE_NAME, PIPELINE_TITLE, REVISION, null, metrics, runtimeInfo, new EventListenerManager()),  new RuleDefinitionsConfigBean(),
        new HashMap<String, Object>(), dataRuleDefinition, new Configuration(), null, null);
    evaluateRule(dataRuleEvaluator, lane);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testNoExceptionInvalidExpression() {
    String lane = "testNoExceptionInvalidExpression";
    //Missing "}"
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testNoExceptionInvalidExpression",
      "testNoExceptionInvalidExpression", lane,
      100, 10, "${record:value(\"/name\")==null", true, "testNoExceptionInvalidExpression", ThresholdType.COUNT, "2",
      5, true, false, true, System.currentTimeMillis());
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator( "name", "0", metrics,
      new AlertManager(PIPELINE_NAME, PIPELINE_TITLE, REVISION, null, metrics, runtimeInfo, null),  new RuleDefinitionsConfigBean(), new HashMap<String, Object>(),
        dataRuleDefinition, new Configuration(), null, null);
    evaluateRule(dataRuleEvaluator, lane);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("exceptionMessage"));
  }

  @Test
  public void testNoAlertRaisedCount() {
    String lane = "testNoAlertRaisedCount";
    //Threshold value is set to 3 but the generated snapshot meets criteria 2. Therefore no alerts
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testNoAlertRaisedCount", "testNoAlertRaisedCount",
      lane, 100, 10, "${record:value(\"/name\")==null}", true, "testNoAlertRaisedCount", ThresholdType.COUNT, "3",
      5, true, false, true, System.currentTimeMillis());
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator( "name", "0", metrics,
      new AlertManager(PIPELINE_NAME, PIPELINE_TITLE, REVISION, null, metrics, runtimeInfo, new EventListenerManager()), new RuleDefinitionsConfigBean(),
        new HashMap<String, Object>(), dataRuleDefinition, new Configuration(), null, null);
    evaluateRule(dataRuleEvaluator, lane);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testAlertRaisedPercentage() {
    String lane = "testAlertRaisedPercentage";
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testAlertRaisedPercentage",
      "testAlertRaisedPercentage", lane,
      100, 10, "${record:value(\"/name\")==null}", true, "testAlertRaisedPercentage", ThresholdType.PERCENTAGE, "40",
      5, true, false, true, System.currentTimeMillis());
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator( "name", "0", metrics,
      new AlertManager(PIPELINE_NAME, PIPELINE_TITLE, REVISION, null, metrics, runtimeInfo, new EventListenerManager()), new RuleDefinitionsConfigBean(),
        new HashMap<String, Object>(), dataRuleDefinition, new Configuration(), null, null);
    evaluateRule(dataRuleEvaluator, lane);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));
  }

  @Test
  public void testNoAlertRaisedPercentage() {
    String lane = "testNoAlertRaisedPercentage";
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testNoAlertRaisedPercentage",
      "testNoAlertRaisedPercentage", lane, 100, 10, "${record:value(\"/name\")==null}", true,
      "testNoAlertRaisedPercentage", ThresholdType.PERCENTAGE, "60", 5, true, false, true, System.currentTimeMillis());
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator( "name", "0", metrics,  null, new RuleDefinitionsConfigBean(),
        new HashMap<String, Object>(), dataRuleDefinition, new Configuration(), null, null);
    evaluateRule(dataRuleEvaluator, lane);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testGaugeChange() {
    String lane = "testGaugeChange";
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testGaugeChange", "testGaugeChange", lane,
      100, 10, "${record:value(\"/name\")==null}", true, "testGaugeChange", ThresholdType.COUNT, "2", 5, true,
      false, true, System.currentTimeMillis());
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator( "name", "0", metrics,
      new AlertManager(PIPELINE_NAME, PIPELINE_TITLE, REVISION, null, metrics, runtimeInfo, new EventListenerManager()), new RuleDefinitionsConfigBean(),
        new HashMap<String, Object>(), dataRuleDefinition, new Configuration(), null, null);

    evaluateRule(dataRuleEvaluator, lane);

    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertEquals(1, ((List)((Map<String, Object>) gauge.getValue()).get("alertTexts")).size());

    evaluateRule(dataRuleEvaluator, lane);

    gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 6, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    // alerts texts is jut one since we throw away new text if its same as the old text
    Assert.assertEquals(1, ((List)((Map<String, Object>) gauge.getValue()).get("alertTexts")).size());

  }

  @Test
  public void testMultipleAlertEnabledMeterEnabled() {
    String lane = "testAlertEnabledMeterEnabled";

    DataRuleDefinition nameNotNull = new DataRuleDefinition("nameNotNull",
      "nameNotNull", lane, 100, 10, "${record:value(\"/name\")==null}", true,
      "nameNotNull", ThresholdType.COUNT, "2", 5, true, false, true, System.currentTimeMillis());
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator( "name", "0", metrics,
      new AlertManager(PIPELINE_NAME, PIPELINE_TITLE, REVISION, null, metrics, runtimeInfo, new EventListenerManager()), new RuleDefinitionsConfigBean(),
        new HashMap<String, Object>(), nameNotNull, new Configuration(), null, null);

    evaluateRule(dataRuleEvaluator, lane);

    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(nameNotNull.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));

    DataRuleDefinition nameEqualsStreamSets = new DataRuleDefinition("nameEqualsStreamSets",
      "nameEqualsStreamSets", lane, 100, 10, "${record:value(\"/zip\")==94101}", true,
      "nameEqualsStreamSets", ThresholdType.COUNT, "1", 5, true, false, true, System.currentTimeMillis());
    DataRuleEvaluator dataRuleEvaluator2 = new DataRuleEvaluator( "name", "0", metrics,
      new AlertManager(PIPELINE_NAME, PIPELINE_TITLE, REVISION, null, metrics, runtimeInfo, new EventListenerManager()), new RuleDefinitionsConfigBean(),
        new HashMap<String, Object>(), nameEqualsStreamSets, new Configuration(), null, null);

    evaluateRule(dataRuleEvaluator2, lane);

    Gauge<Object> gauge2 = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(nameEqualsStreamSets.getId()));
    Assert.assertNotNull(gauge2);
    Assert.assertEquals((long) 2, ((Map<String, Object>) gauge2.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge2.getValue()).get("timestamp"));

  }

  private void evaluateRule(DataRuleEvaluator dataRuleEvaluator, String lane) {
    dataRuleEvaluator.evaluateRule(TestUtil.createSnapshot(lane, dataRuleEvaluator.getDataRuleDefinition().getId())
      .get(LaneResolver.getPostFixedLaneForObserver(lane)).get(dataRuleEvaluator.getDataRuleDefinition().getId()),
      lane, new HashMap<String,EvictingQueue<SampledRecord>>());
  }

  @Test
  public void testDriftEvaluationAndAlert() {
    DriftRuleDefinition def = new DriftRuleDefinition(
        "id",
        "label",
        "lane",
        100,
        10,
        "${drift:size('/', true)}",
        true,
        "alert ${alert:info()}",
        true,
        false,
        true,
        System.currentTimeMillis()
    );
    AlertManager alertManager = Mockito.mock(AlertManager.class);
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator(
        "name",
        "0",
        metrics,
        alertManager,
        new RuleDefinitionsConfigBean(),
        new HashMap<String, Object>(),
        def,
        new Configuration(),
        null,
        null
    );
    ELVariables elVars = new ELVariables();
    elVars.addContextVariable(DataRuleEvaluator.PIPELINE_CONTEXT, new HashMap());

    Record record = new RecordImpl("creator", "id", null, null);
    record.set(Field.create(new ArrayList<Field>()));
    Assert.assertFalse(dataRuleEvaluator.evaluate(elVars, record, def.getCondition(), "id"));
    Assert.assertEquals("alert ", dataRuleEvaluator.resolveAlertText(elVars, record, def));
    Assert.assertFalse(dataRuleEvaluator.evaluate(elVars, record, def.getCondition(), "id"));
    Assert.assertEquals("alert ", dataRuleEvaluator.resolveAlertText(elVars, record, def));
    record.get().getValueAsList().add(Field.create("Hello"));
    Assert.assertTrue(dataRuleEvaluator.evaluate(elVars, record, def.getCondition(), "id"));
    Assert.assertNotEquals("alert ", dataRuleEvaluator.resolveAlertText(elVars, record, def));
    Assert.assertTrue(dataRuleEvaluator.resolveAlertText(elVars, record, def).startsWith("alert "));

    Record record1 = new RecordImpl("creator", "id", null, null);
    record1.set(Field.create(new ArrayList<Field>()));
    Record record2 = new RecordImpl("creator", "id", null, null);
    record2.set(Field.create(new ArrayList<Field>()));
    record2.get().getValueAsList().add(Field.create("Hello"));
    List<Record> records = ImmutableList.of(record1, record2);

    Mockito.verifyZeroInteractions(alertManager);

    ArgumentCaptor<DataRuleDefinition> captor = ArgumentCaptor.forClass(DataRuleDefinition.class);

    dataRuleEvaluator.evaluateRule(records, "lane", new HashMap<String, EvictingQueue<SampledRecord>>());
    Mockito.verify(alertManager).alert(
        Mockito.any(Object.class),
        Mockito.any(),
        captor.capture()
    );
    Assert.assertNotEquals("alert ", captor.getValue().getAlertText());
    Assert.assertTrue(captor.getValue().getAlertText().startsWith("alert "));
  }

  @Test
  public void testAlertMessageWithRecordEL() {
    DriftRuleDefinition def = new DriftRuleDefinition(
        "id",
        "label",
        "lane",
        100,
        10,
        "${drift:size('/', true)}",
        true,
        "alert ${record:id()}",
        true,
        false,
        true,
        System.currentTimeMillis()
    );
    AlertManager alertManager = Mockito.mock(AlertManager.class);
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator(
        "name",
        "0",
        metrics,
        alertManager,
        new RuleDefinitionsConfigBean(),
        new HashMap<String, Object>(),
        def,
        new Configuration(),
        null,
        null
    );
    ELVariables elVars = new ELVariables();
    elVars.addContextVariable(DataRuleEvaluator.PIPELINE_CONTEXT, new HashMap());
    Record record = new RecordImpl("creator", "id", null, null);
    record.set(Field.create(new ArrayList<Field>()));
    Assert.assertFalse(dataRuleEvaluator.evaluate(elVars, record, def.getCondition(), "id"));
    Assert.assertEquals("alert id", dataRuleEvaluator.resolveAlertText(elVars, record, def));
  }

}
