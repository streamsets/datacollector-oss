/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.main.RuntimeInfo;
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
  private ProductionPipelineManagerTask manager;

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty(RuntimeInfo.DATA_DIR, "target/var");
    File f = new File(System.getProperty(RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    TestUtil.captureStagesForProductionRun();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    System.getProperties().remove(RuntimeInfo.DATA_DIR);
  }

  @Before
  public void setUp() throws IOException, PipelineManagerException {
    File f = new File(System.getProperty(RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    ObjectGraph g = ObjectGraph.create(TestUtil.TestProdManagerModule.class);
    manager = g.get(ProductionPipelineManagerTask.class);
    manager.init();
    manager.setState(MY_PIPELINE, PIPELINE_REV, State.STOPPED, "Pipeline created");
    manager.getStateTracker().register(MY_PIPELINE, PIPELINE_REV);
  }

  @After
  public void tearDown() throws InterruptedException, PipelineManagerException {
    TestUtil.stopPipelineIfNeeded(manager);
    LogUtil.unregisterAllLoggers();
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

}
