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
package com.streamsets.datacollector.metrics;

import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.callback.CallbackObjectType;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;
import com.streamsets.datacollector.execution.metrics.MetricsEventRunnable;
import com.streamsets.datacollector.execution.runner.cluster.SlaveCallbackManager;
import com.streamsets.datacollector.execution.runner.common.ThreadHealthReporter;
import com.streamsets.datacollector.event.json.CounterJson;
import com.streamsets.datacollector.event.json.MeterJson;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.ExecutionMode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

public class TestMetricsAggregation {
  Collection<CallbackInfo> callbackInfoCollection;
  PipelineStateStore pipelineStateStore;
  SlaveCallbackManager slaveCallbackManager;

  @Before
  public void setup() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();

    callbackInfoCollection = new LinkedList<>();

    callbackInfoCollection.add(new CallbackInfo(null, null, null, null, "worker1", "", null, null, null, null,
        CallbackObjectType.METRICS, readFile(classLoader.getResource("metrics/metrics1.json").getFile()), null));

    callbackInfoCollection.add(new CallbackInfo(null, null, null, null, "worker2", "", null, null, null, null,
        CallbackObjectType.METRICS, readFile(classLoader.getResource("metrics/metrics2.json").getFile()), null));

    callbackInfoCollection.add(new CallbackInfo(null, null, null, null, "worker3", "", null, null, null, null,
        CallbackObjectType.METRICS, readFile(classLoader.getResource("metrics/metrics3.json").getFile()), null));

    callbackInfoCollection.add(new CallbackInfo(null, null, null, null, "worker4", "", null, null, null, null,
        CallbackObjectType.METRICS, readFile(classLoader.getResource("metrics/metrics4.json").getFile()), null));

    slaveCallbackManager = Mockito.mock(SlaveCallbackManager.class);
    Mockito.when(slaveCallbackManager.getSlaveCallbackList(CallbackObjectType.METRICS))
      .thenReturn(callbackInfoCollection);

    pipelineStateStore = Mockito.mock(PipelineStateStore.class);
    Mockito.when(pipelineStateStore.getState(Matchers.anyString(), Matchers.anyString())).thenReturn(
      new PipelineStateImpl("aaa", "samplePipeline", "1.0.0",
        PipelineStatus.RUNNING, "The pipeline is not running", System.currentTimeMillis(), null, ExecutionMode.CLUSTER_BATCH, null, 0, 0)
    );


  }

  private String readFile(String path) throws IOException {
    byte[] encoded = Files.readAllBytes(Paths.get(path));
    return new String(encoded, StandardCharsets.UTF_8);
  }

  @Test
  public void testAggregatedMetrics() {
    MetricsEventRunnable metricsEventRunnable = new MetricsEventRunnable(
        "a",
        "0",
        new Configuration(),
        pipelineStateStore,
        Mockito.mock(ThreadHealthReporter.class),
        new EventListenerManager(),
        null,
        slaveCallbackManager,
        null
    );
    MetricRegistryJson aggregatedMetrics = metricsEventRunnable.getAggregatedMetrics();
    validateAggregatedResults(aggregatedMetrics);

    //Calling twice should not cause issue
    aggregatedMetrics = metricsEventRunnable.getAggregatedMetrics();
    validateAggregatedResults(aggregatedMetrics);

    aggregatedMetrics = metricsEventRunnable.getAggregatedMetrics();
    validateAggregatedResults(aggregatedMetrics);
  }

  private void validateAggregatedResults(MetricRegistryJson aggregatedMetrics) {
    Map<String, CounterJson> counters = aggregatedMetrics.getCounters();
    CounterJson randomSourceOutputCounter =
      counters.get("stage.com_streamsets_pipeline_stage_devtest_RandomSource1432492071220.outputRecords.counter");
    Assert.assertEquals(616197, randomSourceOutputCounter.getCount());


    CounterJson randomErrorOutputCounter =
      counters.get("stage.com_streamsets_pipeline_stage_devtest_RandomErrorProcessor1432492075926.outputRecords.counter");
    Assert.assertEquals(433000, randomErrorOutputCounter.getCount());


    CounterJson randomErrorStageErrorsCounter =
      counters.get("stage.com_streamsets_pipeline_stage_devtest_RandomErrorProcessor1432492075926.stageErrors.counter");
    Assert.assertEquals(314, randomErrorStageErrorsCounter.getCount());


    CounterJson randomErrorErrorCounter =
      counters.get("stage.com_streamsets_pipeline_stage_devtest_RandomErrorProcessor1432492075926.errorRecords.counter");
    Assert.assertEquals(100884, randomErrorErrorCounter.getCount());


    Map<String, MeterJson> meters = aggregatedMetrics.getMeters();
    MeterJson randomErrorOutputMeter =
      meters.get("stage.com_streamsets_pipeline_stage_devtest_RandomErrorProcessor1432492075926.outputRecords.meter");
    Assert.assertEquals(6249 + 23762 + 49807 + 353182, randomErrorOutputMeter.getCount());
    Assert.assertEquals(551.0 + 601.7606745367777 + 643.0552365961045 + 692.4152915549353,
      randomErrorOutputMeter.getM1_rate(), 2);
    Assert.assertEquals(551.0 + 562.8896436480533 + 578.2306638834726 + 664.5399755279977,
      randomErrorOutputMeter.getM5_rate(), 2);
    Assert.assertEquals(551.0 + 555.0726441281585 + 560.7501876460198 + 610.9599334980452,
      randomErrorOutputMeter.getM15_rate(), 2);
    Assert.assertEquals(551.0 + 553.0503183959565 + 555.9644961786971 + 585.1784987905182,
      randomErrorOutputMeter.getM30_rate(), 2);

    Assert.assertEquals(551.0 + 552.0286825605679 + 553.5050116409752 + 569.292007813068,
      randomErrorOutputMeter.getH1_rate(), 2);
    Assert.assertEquals(551.0 + 551.1719385238548 + 551.4206992117256 + 554.2305473174828,
      randomErrorOutputMeter.getH6_rate(), 2);
    Assert.assertEquals(551.0 + 551.0859938854843 + 551.2105103689343 + 552.6247638790022,
      randomErrorOutputMeter.getH12_rate(), 2);
    Assert.assertEquals(551.0 + 551.0430031004082 + 551.1052954060336 + 551.8147684623682,
      randomErrorOutputMeter.getH24_rate(), 2);

    Assert.assertEquals(649.0539505737786 + 683.3633772998223 + 679.0145974772339 + 689.7506348259288,
      randomErrorOutputMeter.getMean_rate(), 2);
  }

}
