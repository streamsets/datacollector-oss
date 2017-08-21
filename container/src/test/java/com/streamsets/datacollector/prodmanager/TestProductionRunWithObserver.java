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
package com.streamsets.datacollector.prodmanager;
/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 *//*
package com.streamsets.pipeline.prodmanager;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.streamsets.pipeline.alerts.AlertsUtil;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.LogUtil;
import com.streamsets.pipeline.util.TestUtil;
import dagger.ObjectGraph;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestProductionRunWithObserver {

  private static final String MY_PIPELINE = "my pipeline";
  private static final String PIPELINE_REV = "2.0";
  private static final String MY_PROCESSOR = "p";
  private StandalonePipelineManagerTask manager;

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, "target/var");
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    TestUtil.captureStagesForProductionRun();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  @Before
  public void setUp() throws IOException, PipelineManagerException {
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    ObjectGraph g = ObjectGraph.create(TestUtil.TestProdManagerModule.class);
    manager = g.get(StandalonePipelineManagerTask.class);
    manager.init();
    manager.setState(MY_PIPELINE, PIPELINE_REV, State.STOPPED, "Pipeline created", null);
    manager.getStateTracker().register(MY_PIPELINE, PIPELINE_REV);
  }

  @After
  public void tearDown() throws InterruptedException, PipelineManagerException {
    TestUtil.stopPipelineIfNeeded(manager);
    manager.stop();
  }

  @Test()
  public void testStartStopPipeline() throws PipelineStoreException, PipelineManagerException,
      PipelineRuntimeException, StageException {
    PipelineState pipelineState = manager.startPipeline(MY_PIPELINE, PIPELINE_REV);
    Assert.assertEquals(State.RUNNING, pipelineState.getState());
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());

    pipelineState = manager.stopPipeline(false);
    Assert.assertEquals(State.STOPPING, pipelineState.getState());
    //The pipeline could be stopping or has already been stopped by now
    Assert.assertTrue(manager.getPipelineState().getState() == State.STOPPING ||
        manager.getPipelineState().getState() == State.STOPPED);
  }

  @Test()
  *//**
   * Tests pipeline with alerts. The triggered alert is then deleted and the counter is verified.
   * Potentially flaky test - DataRuleDefinition has high threshold value to ensure it runs fine.
   *//*
  public void testPipelineWithAlerts() throws PipelineStoreException, PipelineManagerException,
    PipelineRuntimeException, StageException, InterruptedException {
    PipelineState pipelineState = manager.startPipeline(MY_PIPELINE, PIPELINE_REV);
    Assert.assertEquals(State.RUNNING, pipelineState.getState());
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());

    Gauge alertGauge = MetricsConfigurator.getGauge(manager.getMetrics(), AlertsUtil.getAlertGaugeName("myID"));
    //wait until alert triggers
    while(alertGauge == null) {
      Thread.sleep(200);
      alertGauge = MetricsConfigurator.getGauge(manager.getMetrics(), AlertsUtil.getAlertGaugeName("myID"));
    }

    //the counter as of now should be >= 100
    Counter counter = MetricsConfigurator.getCounter(manager.getMetrics(), AlertsUtil.getUserMetricName("myID"));
    Assert.assertTrue(counter.getCount() >= 100);

    //delete alert
    manager.deleteAlert(MY_PIPELINE, PIPELINE_REV, "myID");
    alertGauge = MetricsConfigurator.getGauge(manager.getMetrics(), AlertsUtil.getAlertGaugeName("myID"));
    counter = MetricsConfigurator.getCounter(manager.getMetrics(), AlertsUtil.getUserMetricName("myID"));

    //gauge should be null and counter should be 0
    //But since the pipeline is still running counter could be > 0.
    Assert.assertTrue(alertGauge == null);
    Assert.assertTrue(counter.getCount() < 100);

    pipelineState = manager.stopPipeline(false);

    Assert.assertEquals(State.STOPPING, pipelineState.getState());
    //The pipeline could be stopping or has already been stopped by now
    Assert.assertTrue(manager.getPipelineState().getState() == State.STOPPING ||
      manager.getPipelineState().getState() == State.STOPPED);
  }

}
*/
