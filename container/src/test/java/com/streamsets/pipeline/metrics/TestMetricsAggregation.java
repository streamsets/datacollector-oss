/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.pipeline.metrics;

import com.streamsets.pipeline.callback.CallbackInfo;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.prodmanager.PipelineManager;
import com.streamsets.pipeline.prodmanager.PipelineState;
import com.streamsets.pipeline.prodmanager.State;
import com.streamsets.pipeline.restapi.bean.CounterJson;
import com.streamsets.pipeline.restapi.bean.MeterJson;
import com.streamsets.pipeline.restapi.bean.MetricRegistryJson;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class TestMetricsAggregation {
  PipelineManager pipelineManager;
  RuntimeInfo runtimeInfo;
  Collection<CallbackInfo> callbackInfoCollection;

  @Before
  public void setup() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();

    callbackInfoCollection = new HashSet<>();

    callbackInfoCollection.add(new CallbackInfo(null, "worker1", null, null, null, null, null,
      readFile(classLoader.getResource("metrics/metrics1.json").getFile())));

    callbackInfoCollection.add(new CallbackInfo(null, "worker2", null, null, null, null, null,
      readFile(classLoader.getResource("metrics/metrics2.json").getFile())));

    callbackInfoCollection.add(new CallbackInfo(null, "worker3", null, null, null, null, null,
      readFile(classLoader.getResource("metrics/metrics3.json").getFile())));

    callbackInfoCollection.add(new CallbackInfo(null, "worker4", null, null, null, null, null,
      readFile(classLoader.getResource("metrics/metrics4.json").getFile())));


    pipelineManager = Mockito.mock(PipelineManager.class);
    Mockito.when(pipelineManager.getPipelineState())
      .thenReturn(new PipelineState("samplePipeline", "1.0.0",
        State.RUNNING, "The pipeline is not running", System.currentTimeMillis(), null, null));

    Mockito.when(pipelineManager.getSlaveCallbackList())
      .thenReturn(callbackInfoCollection);

    runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getExecutionMode())
      .thenReturn(RuntimeInfo.ExecutionMode.CLUSTER);

  }

  private String readFile(String path) throws IOException {
    byte[] encoded = Files.readAllBytes(Paths.get(path));
    return new String(encoded, StandardCharsets.UTF_8);
  }

  @Test
  public void testAggregatedMetrics() {
    MetricsEventRunnable metricsEventRunnable = new MetricsEventRunnable(pipelineManager, runtimeInfo, 2000);
    MetricRegistryJson aggregatedMetrics = metricsEventRunnable.getAggregatedMetrics();

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
