/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.LogUtil;
import com.streamsets.pipeline.util.TestUtil;
import dagger.ObjectGraph;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class TestProductionRun {

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

  @Test(expected = PipelineRuntimeException.class)
  public void testStartInvalidPipeline() throws PipelineStoreException, PipelineManagerException,
      PipelineRuntimeException, StageException {
    //cannot run pipeline "xyz", the default created by FilePipelineStore.init() as it is not valid
    manager.startPipeline("invalid", PIPELINE_REV);
  }

  @Test(expected = PipelineManagerException.class)
  public void testStartNonExistingPipeline() throws PipelineStoreException, PipelineManagerException,
      PipelineRuntimeException, StageException {
    //pipeline "abc" does not exist
    manager.startPipeline("abc", PIPELINE_REV);
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

  @Test
  public void testStopManager() throws PipelineManagerException, StageException, PipelineRuntimeException,
      PipelineStoreException, InterruptedException {
    //Set state to running
    manager.startPipeline(MY_PIPELINE, PIPELINE_REV);
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());

    manager.stop();
    Assert.assertTrue(manager.getPipelineState().getState() == State.STOPPING ||
        manager.getPipelineState().getState() == State.NODE_PROCESS_SHUTDOWN);

    waitForNodeProcessShutdown();

    manager.init();
    //Stopping the pipeline also stops the pipeline
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());

  }

  @Test
  public void testCaptureSnapshot() throws PipelineStoreException, PipelineManagerException, PipelineRuntimeException,
      StageException, InterruptedException {
    manager.startPipeline(MY_PIPELINE, PIPELINE_REV);
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());

    manager.captureSnapshot(10);

    waitForSnapshot();

    SnapshotStatus snapshotStatus = manager.getSnapshotStatus();
    Assert.assertEquals(true, snapshotStatus.isExists());
    Assert.assertEquals(false, snapshotStatus.isSnapshotInProgress());

    InputStream snapshot = manager.getSnapshot(MY_PIPELINE, PIPELINE_REV);
    //TODO: read the input snapshot into String format and Use de-serializer when ready

    manager.deleteSnapshot(MY_PIPELINE, PIPELINE_REV);
    snapshotStatus = manager.getSnapshotStatus();
    Assert.assertEquals(false, snapshotStatus.isExists());
    Assert.assertEquals(false, snapshotStatus.isSnapshotInProgress());

  }

  @Test()
  public void testGetHistory() throws PipelineStoreException, PipelineManagerException, PipelineRuntimeException, StageException, InterruptedException {
    manager.startPipeline(MY_PIPELINE, PIPELINE_REV);
    manager.stopPipeline(false);

    waitForPipelineToStop();

    manager.startPipeline(MY_PIPELINE, PIPELINE_REV);
    manager.stopPipeline(false);

    waitForPipelineToStop();

    manager.startPipeline(MY_PIPELINE, PIPELINE_REV);
    manager.stopPipeline(false);

    waitForPipelineToStop();

    List<PipelineState> pipelineStates = manager.getHistory(MY_PIPELINE, PIPELINE_REV, false);
    Assert.assertEquals(6, pipelineStates.size());

    //make sure that the history is returned in the LIFO order
    //Also note that State.STOPPING is not a persisted state
    Assert.assertEquals(State.STOPPED, pipelineStates.get(0).getState());
    Assert.assertEquals(State.RUNNING, pipelineStates.get(1).getState());
    Assert.assertEquals(State.STOPPED, pipelineStates.get(2).getState());
    Assert.assertEquals(State.RUNNING, pipelineStates.get(3).getState());
    Assert.assertEquals(State.STOPPED, pipelineStates.get(4).getState());
    Assert.assertEquals(State.RUNNING, pipelineStates.get(5).getState());

  }

  @Test(expected = PipelineManagerException.class)
  public void testGetHistoryNonExistingPipeline() throws PipelineStoreException, PipelineManagerException,
      PipelineRuntimeException, StageException, InterruptedException {
    manager.startPipeline(MY_PIPELINE, PIPELINE_REV);
    manager.stopPipeline(false);

    waitForPipelineToStop();

    manager.getHistory("nonExistingPipeline", PIPELINE_REV, true);
  }

  @Test
  public void testGetHistoryPipelineNeverRun() throws PipelineStoreException, PipelineManagerException,
      PipelineRuntimeException, StageException, InterruptedException {
    List<PipelineState> pipelineStates = manager.getHistory(MY_PIPELINE, PIPELINE_REV, true);
    Assert.assertEquals(0, pipelineStates.size());
  }

  @Test
  public void testStartManagerAfterKill() throws IOException {
    manager.stop();
    //copy pre-created pipelineState.json into the state directory and start manager
    InputStream in = getClass().getClassLoader().getResourceAsStream("testStartManagerAfterKill.json");
    File f = new File(new File(System.getProperty(RuntimeInfo.DATA_DIR), "runInfo") , "pipelineState.json");
    OutputStream out = new FileOutputStream(f);
    IOUtils.copy(in, out);
    in.close();
    out.flush();
    out.close();

    //The pre-created json has "myPipeline" RUNNING
    manager.init();
    PipelineState pipelineState = manager.getPipelineState();
    Assert.assertEquals(MY_PIPELINE, pipelineState.getName());
    Assert.assertEquals(State.RUNNING, pipelineState.getState());
  }

  @Test
  public void testGetErrorRecords() throws PipelineStoreException, StageException, PipelineManagerException,
    PipelineRuntimeException, InterruptedException, IOException {

    manager.startPipeline(MY_PIPELINE, PIPELINE_REV);
    waitForErrorRecords(MY_PROCESSOR);

    List<Record> errorRecords = manager.getErrorRecords(MY_PROCESSOR, 100);
    Assert.assertNotNull(errorRecords);
    Assert.assertEquals(false, errorRecords.isEmpty());

    manager.stopPipeline(false);
    waitForPipelineToStop();
  }

  @Test
  public void testDeleteErrorRecords() throws PipelineStoreException, StageException, PipelineManagerException,
    PipelineRuntimeException, InterruptedException, IOException {
    manager.startPipeline(MY_PIPELINE, PIPELINE_REV);
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());

    waitForErrorRecords(MY_PROCESSOR);

    List<Record> errorRecords = manager.getErrorRecords(MY_PROCESSOR, 100);
    Assert.assertNotNull(errorRecords);
    Assert.assertEquals(false, errorRecords.isEmpty());

    manager.stopPipeline(false);
    waitForPipelineToStop();
  }

  @Test
  public void testGetMetrics() throws PipelineStoreException, StageException, PipelineManagerException,
      PipelineRuntimeException, InterruptedException {
    manager.startPipeline(MY_PIPELINE, PIPELINE_REV);
    Thread.sleep(50);
    MetricRegistry metrics = manager.getMetrics();
    Assert.assertNotNull(metrics);

  }

  @Test
  public void testResetOffset() throws PipelineStoreException, InterruptedException, PipelineManagerException,
      StageException, PipelineRuntimeException {
    manager.startPipeline(MY_PIPELINE, PIPELINE_REV);
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());
    waitForErrorRecords(MY_PROCESSOR);
    manager.stopPipeline(false);
    waitForPipelineToStop();

    Assert.assertNotNull(manager.getOffset());

    manager.resetOffset(MY_PIPELINE, PIPELINE_REV);
    Assert.assertNull(manager.getOffset());
  }

  @Test(expected = PipelineManagerException.class)
  public void testResetOffsetWhileRunning() throws PipelineStoreException, InterruptedException, PipelineManagerException,
    StageException, PipelineRuntimeException {
    manager.startPipeline(MY_PIPELINE, PIPELINE_REV);
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());

    manager.resetOffset(MY_PIPELINE, PIPELINE_REV);

  }

  @Test
  public void testGetErrorMessages() throws PipelineStoreException, StageException, PipelineManagerException,
    PipelineRuntimeException, InterruptedException, IOException {

    manager.startPipeline(MY_PIPELINE, PIPELINE_REV);
    waitForErrorMessages(MY_PROCESSOR);

    List<ErrorMessage> errorMessages = manager.getErrorMessages(MY_PROCESSOR, 100);
    Assert.assertNotNull(errorMessages);
    Assert.assertEquals(false, errorMessages.isEmpty());

    manager.stopPipeline(false);
    waitForPipelineToStop();
  }

  //TODO:
  //Add tests which create multiple pipelines and runs one of them. Query snapshot, status, history etc on
  //the running as well as other pipelines

  /*********************************************/
  /*********************************************/

  private void waitForPipelineToStop() throws InterruptedException {
    while(manager.getPipelineState().getState() != State.STOPPED) {
      Thread.sleep(5);
    }
  }

  private void waitForNodeProcessShutdown() throws InterruptedException {
    while(manager.getPipelineState().getState() != State.NODE_PROCESS_SHUTDOWN) {
      Thread.sleep(5);
    }
  }

  private void waitForSnapshot() throws InterruptedException {
    while(!manager.getSnapshotStatus().isExists()) {
      Thread.sleep(5);
    }
  }

  private void waitForErrorRecords(String instanceName) throws InterruptedException, PipelineManagerException {
    while(manager.getErrorRecords(instanceName, 100).isEmpty()) {
      Thread.sleep(5);
    }
  }

  private void waitForErrorMessages(String instanceName) throws InterruptedException, PipelineManagerException {
    while(manager.getErrorMessages(instanceName, 100).isEmpty()) {
      Thread.sleep(5);
    }
  }

}
