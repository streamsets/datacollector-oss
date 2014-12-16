/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.store.impl.FilePipelineStoreTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.TestUtil;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;

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
import java.util.Arrays;
import java.util.List;

public class TestProductionRun {

  private static final String MY_PIPELINE = "myPipeline";
  private static final String MY_PROCESSOR = "p";
  private ProductionPipelineManagerTask manager;

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty("pipeline.data.dir", "target/var");
    File f = new File(System.getProperty("pipeline.data.dir"));
    FileUtils.deleteDirectory(f);
    TestUtil.captureStagesForProductionRun();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    System.getProperties().remove("pipeline.data.dir");
  }

  @Before
  public void setUp() throws IOException {

    File f = new File(System.getProperty("pipeline.data.dir"));
    FileUtils.deleteDirectory(f);

    ObjectGraph g = ObjectGraph.create(TestProdManagerModule.class);
    manager = g.get(ProductionPipelineManagerTask.class);
    manager.init();
  }

  @After
  public void tearDown() throws InterruptedException, PipelineManagerException {
    stopPipelineIfNeeded();
    manager.stop();
  }

  @Test(expected = PipelineRuntimeException.class)
  public void testStartInvalidPipeline() throws PipelineStoreException, PipelineManagerException,
      PipelineRuntimeException, StageException {
    //cannot run pipeline "xyz", the default created by FilePipelineStore.init() as it is not valid
    manager.startPipeline("xyz", "0");
  }

  @Test(expected = PipelineStoreException.class)
  public void testStartNonExistingPipeline() throws PipelineStoreException, PipelineManagerException,
      PipelineRuntimeException, StageException {
    //pipeline "abc" does not exist
    manager.startPipeline("abc", "0");
  }

  @Test()
  public void testStartStopPipeline() throws PipelineStoreException, PipelineManagerException,
      PipelineRuntimeException, StageException {
    PipelineState pipelineState = manager.startPipeline(MY_PIPELINE, "0");
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
    manager.startPipeline(MY_PIPELINE, "0");
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
    manager.startPipeline(MY_PIPELINE, "0");
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());

    manager.captureSnapshot(10);

    waitForSnapshot();

    SnapshotStatus snapshotStatus = manager.getSnapshotStatus();
    Assert.assertEquals(true, snapshotStatus.isExists());
    Assert.assertEquals(false, snapshotStatus.isSnapshotInProgress());

    InputStream snapshot = manager.getSnapshot(MY_PIPELINE);
    //TODO: read the input snapshot into String format and Use de-serializer when ready

    manager.deleteSnapshot(MY_PIPELINE);
    snapshotStatus = manager.getSnapshotStatus();
    Assert.assertEquals(false, snapshotStatus.isExists());
    Assert.assertEquals(false, snapshotStatus.isSnapshotInProgress());

  }

  @Test()
  public void testGetHistory() throws PipelineStoreException, PipelineManagerException, PipelineRuntimeException, StageException, InterruptedException {
    manager.startPipeline(MY_PIPELINE, "0");
    manager.stopPipeline(false);

    waitForPipelineToStop();

    manager.startPipeline(MY_PIPELINE, "0");
    manager.stopPipeline(false);

    waitForPipelineToStop();

    manager.startPipeline(MY_PIPELINE, "0");
    manager.stopPipeline(false);

    waitForPipelineToStop();

    List<PipelineState> pipelineStates = manager.getHistory(MY_PIPELINE);
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
    manager.startPipeline(MY_PIPELINE, "0");
    manager.stopPipeline(false);

    waitForPipelineToStop();

    manager.getHistory("nonExistingPipeline");
  }

  @Test
  public void testGetHistoryPipelineNeverRun() throws PipelineStoreException, PipelineManagerException,
      PipelineRuntimeException, StageException, InterruptedException {
    List<PipelineState> pipelineStates = manager.getHistory(MY_PIPELINE);
    Assert.assertEquals(0, pipelineStates.size());
  }

  @Test
  public void testStartManagerAfterKill() throws IOException {
    manager.stop();
    //copy pre-created pipelineState.json into the state directory and start manager
    InputStream in = getClass().getClassLoader().getResourceAsStream("testStartManagerAfterKill.json");
    File f = new File(new File(System.getProperty("pipeline.data.dir"), "runInfo") , "pipelineState.json");
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
      PipelineRuntimeException, InterruptedException {

    manager.startPipeline(MY_PIPELINE, "0");
    waitForErrorRecords(MY_PROCESSOR);

    List<Record> errorRecords = manager.getErrorRecords(MY_PROCESSOR);
    Assert.assertNotNull(errorRecords);
    Assert.assertEquals(false, errorRecords.isEmpty());

    manager.stopPipeline(false);
    waitForPipelineToStop();

    InputStream erStream = manager.getErrors(MY_PIPELINE, "0");
    Assert.assertNotNull(erStream);
    //TODO: read the input error records into String format and Use Record de-serializer when ready

    //delete the error record file
    manager.deleteErrors(MY_PIPELINE, "0");

    erStream = manager.getErrors(MY_PIPELINE, "0");
    Assert.assertNull(erStream);
  }

  @Test
  public void testDeleteErrorRecords() throws PipelineStoreException, StageException, PipelineManagerException,
      PipelineRuntimeException, InterruptedException {
    manager.startPipeline(MY_PIPELINE, "0");
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());

    waitForErrorRecords(MY_PROCESSOR);

    List<Record> errorRecords = manager.getErrorRecords(MY_PROCESSOR);
    Assert.assertNotNull(errorRecords);
    Assert.assertEquals(false, errorRecords.isEmpty());

    manager.stopPipeline(false);
    waitForPipelineToStop();

    //check there are error records
    InputStream erStream = manager.getErrors(MY_PIPELINE, "0");
    Assert.assertNotNull(erStream);

    manager.deleteErrors(MY_PIPELINE, "0");

    erStream = manager.getErrors(MY_PIPELINE, "0");
    //verify there are no records
    Assert.assertNull(erStream);
  }

  @Test
  public void testGetMetrics() throws PipelineStoreException, StageException, PipelineManagerException,
      PipelineRuntimeException, InterruptedException {
    manager.startPipeline(MY_PIPELINE, "0");
    Thread.sleep(50);
    MetricRegistry metrics = manager.getMetrics();
    Assert.assertNotNull(metrics);

  }

  @Test
  public void testSetOffset() throws PipelineStoreException, InterruptedException, PipelineManagerException,
    StageException, PipelineRuntimeException {
    manager.startPipeline(MY_PIPELINE, "0");
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());
    waitForErrorRecords(MY_PROCESSOR);
    manager.stopPipeline(false);
    waitForPipelineToStop();

    Assert.assertNotNull(manager.getOffset());

    manager.setOffset("myOffset");
    Assert.assertEquals("myOffset", manager.getOffset());
  }

  @Test
  public void testResetOffset() throws PipelineStoreException, InterruptedException, PipelineManagerException,
      StageException, PipelineRuntimeException {
    manager.startPipeline(MY_PIPELINE, "0");
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());
    waitForErrorRecords(MY_PROCESSOR);
    manager.stopPipeline(false);
    waitForPipelineToStop();

    Assert.assertNotNull(manager.getOffset());

    manager.resetOffset(MY_PIPELINE);
    Assert.assertNull(manager.getOffset());
  }

  @Test(expected = PipelineManagerException.class)
  public void testResetOffsetWhileRunning() throws PipelineStoreException, InterruptedException, PipelineManagerException,
    StageException, PipelineRuntimeException {
    manager.startPipeline(MY_PIPELINE, "0");
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());

    manager.resetOffset(MY_PIPELINE);

  }

  @Test
  public void testGetErrorMessages() throws PipelineStoreException, StageException, PipelineManagerException,
    PipelineRuntimeException, InterruptedException {

    manager.startPipeline(MY_PIPELINE, "0");
    waitForErrorMessages(MY_PROCESSOR);

    List<ErrorMessage> errorMessages = manager.getErrorMessages(MY_PROCESSOR);
    Assert.assertNotNull(errorMessages);
    Assert.assertEquals(false, errorMessages.isEmpty());

    manager.stopPipeline(false);
    waitForPipelineToStop();

    InputStream erStream = manager.getErrors(MY_PIPELINE, "0");
    Assert.assertNotNull(erStream);
    //TODO: read the input error records into String format and Use Record de-serializer when ready

    //delete the error record file
    manager.deleteErrors(MY_PIPELINE, "0");

    erStream = manager.getErrors(MY_PIPELINE, "0");
    Assert.assertNull(erStream);
  }

  //TODO:
  //Add tests which create multiple pipelines and runs one of them. Query snapshot, status, history etc on
  //the running as well as other pipelines

  /*********************************************/
  /*********************************************/

  @Module(library = true)
  static class TestStageLibraryModule {

    public TestStageLibraryModule() {
    }

    @Provides
    public StageLibraryTask provideStageLibrary() {
      return MockStages.createStageLibrary();
    }
  }

  @Module(library = true, includes = {TestRuntimeModule.class, TestConfigurationModule.class})
  static class TestPipelineStoreModule {

    public TestPipelineStoreModule() {
    }

    @Provides
    public PipelineStoreTask providePipelineStore(RuntimeInfo info, Configuration conf) {
      FilePipelineStoreTask pipelineStoreTask = new FilePipelineStoreTask(info, conf);
      pipelineStoreTask.init();
      try {
        pipelineStoreTask.create(MY_PIPELINE, "description", "tag");
        PipelineConfiguration pipelineConf = pipelineStoreTask.load(MY_PIPELINE, "0");
        PipelineConfiguration mockPipelineConf = MockStages.createPipelineConfigurationSourceProcessorTarget();
        pipelineConf.setStages(mockPipelineConf.getStages());
        pipelineStoreTask.save(MY_PIPELINE, "admin", "tag", "description"
            , pipelineConf);
      } catch (PipelineStoreException e) {
        throw new RuntimeException(e);
      }

      return pipelineStoreTask;
    }
  }

  @Module(library = true)
  static class TestConfigurationModule {

    public TestConfigurationModule() {
    }

    @Provides
    public Configuration provideRuntimeInfo() {
      Configuration conf = new Configuration();
      return conf;
    }
  }

  @Module(library = true)
  static class TestRuntimeModule {

    public TestRuntimeModule() {
    }

    @Provides
    public RuntimeInfo provideRuntimeInfo() {
      RuntimeInfo info = new RuntimeInfo(Arrays.asList(getClass().getClassLoader()));
      return info;
    }
  }

  @Module(injects = ProductionPipelineManagerTask.class
      , library = true, includes = {TestRuntimeModule.class, TestPipelineStoreModule.class
      , TestStageLibraryModule.class, TestConfigurationModule.class})
  static class TestProdManagerModule {

    public TestProdManagerModule() {
    }

    @Provides
    public ProductionPipelineManagerTask provideStateManager(RuntimeInfo runtimeInfo, Configuration configuration
        ,PipelineStoreTask pipelineStore, StageLibraryTask stageLibrary) {
      return new ProductionPipelineManagerTask(runtimeInfo, configuration, pipelineStore, stageLibrary);
    }
  }

  private void stopPipelineIfNeeded() throws InterruptedException, PipelineManagerException {
    if(manager.getPipelineState().getState() == State.RUNNING) {
      manager.stopPipeline(false);
    }

    while(manager.getPipelineState().getState() != State.FINISHED &&
        manager.getPipelineState().getState() != State.STOPPED &&
        manager.getPipelineState().getState() != State.ERROR) {
      Thread.sleep(5);
    }
  }

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
    while(manager.getErrorRecords(instanceName).isEmpty()) {
      Thread.sleep(5);
    }
  }

  private void waitForErrorMessages(String instanceName) throws InterruptedException, PipelineManagerException {
    while(manager.getErrorMessages(instanceName).isEmpty()) {
      Thread.sleep(5);
    }
  }

}
