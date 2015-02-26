/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.prodmanager.PipelineManagerException;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import com.streamsets.pipeline.prodmanager.State;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.snapshotstore.impl.FileSnapshotStore;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.impl.FilePipelineStoreTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.TestUtil;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

public class TestProdPipelineRunnable {

  private static final String PIPELINE_NAME = "xyz";
  private static final String REVISION = "1.0";
  private ProductionPipelineManagerTask manager = null;
  private RuntimeInfo info = null;

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty(RuntimeInfo.DATA_DIR, "./target/var");
    File f = new File(System.getProperty(RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    System.getProperties().remove(RuntimeInfo.DATA_DIR);
  }

  @Before()
  public void setUp() {
    MockStages.resetStageCaptures();
    info = new RuntimeInfo(Arrays.asList(getClass().getClassLoader()));
    manager = new ProductionPipelineManagerTask(info, Mockito.mock(Configuration.class)
        , Mockito.mock(FilePipelineStoreTask.class), Mockito.mock(StageLibraryTask.class));
    manager.init();
  }

  @After
  public void tearDown() {
    manager.stop();
    manager.getStateTracker().getStateFile().delete();
  }

  @Test
  public void testRun() throws Exception {

    TestUtil.captureMockStages();

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true);
    ProductionPipelineRunnable runnable = new ProductionPipelineRunnable(manager, pipeline, PIPELINE_NAME, REVISION,
      Collections.<Future<?>>emptyList());
    runnable.run();

    //The source returns null offset because all the data from source was read
    Assert.assertNull(pipeline.getCommittedOffset());

    Assert.assertTrue(pipeline.getPipeline().getRunner().getBatchesOutput().isEmpty());
  }

  @Test
  public void testStop() throws Exception {

    TestUtil.captureMockStages();

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, false);
    ProductionPipelineRunnable runnable = new ProductionPipelineRunnable(manager, pipeline, PIPELINE_NAME, REVISION,
      Collections.<Future<?>>emptyList());

    runnable.stop(false);
    Assert.assertTrue(pipeline.wasStopped());

    //Stops after the first batch
    runnable.run();

    //Offset 1 expected as pipeline was stopped after the first batch
    Assert.assertEquals("1", pipeline.getCommittedOffset());
    //no output as capture was not set to true
    Assert.assertTrue(pipeline.getPipeline().getRunner().getBatchesOutput().isEmpty());
  }

  private volatile CountDownLatch latch;
  private volatile boolean stopInterrupted;

  @Test
  public void testStopInterrupt() throws Exception {
    latch = new CountDownLatch(1);
    stopInterrupted = false;
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        try {
          latch.countDown();
          Thread.currentThread().sleep(1000000);
        } catch (InterruptedException ex) {
          stopInterrupted = true;
        }
        return null;
      }
    });

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, false);
    ProductionPipelineRunnable runnable = new ProductionPipelineRunnable(manager, pipeline, PIPELINE_NAME, REVISION,
      Collections.<Future<?>>emptyList());

    Thread t = new Thread(runnable);
    t.start();
    latch.await();
    runnable.stop(false);
    t.join();
    Assert.assertTrue(stopInterrupted);
  }

  @Test
  public void testErrorState() throws Exception {
    System.setProperty(RuntimeInfo.DATA_DIR, "./target/var");

    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        throw new RuntimeException("Simulate runtime failure in source");
      }
    });

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true);
    ProductionPipelineRunnable runnable = new ProductionPipelineRunnable(manager, pipeline, PIPELINE_NAME, REVISION,
      Collections.<Future<?>>emptyList());

    //Stops after the first batch
    runnable.run();

    Assert.assertEquals(State.ERROR, manager.getPipelineState().getState());
    //Offset 1 expected as there was a Runtime exception
    Assert.assertEquals("1", pipeline.getCommittedOffset());
    //no output as captured as there was an exception in source
    Assert.assertTrue(pipeline.getPipeline().getRunner().getBatchesOutput().isEmpty());
  }

  private ProductionPipeline createProductionPipeline(DeliveryGuarantee deliveryGuarantee, boolean capturenextBatch)
    throws PipelineRuntimeException, PipelineManagerException, StageException {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");

    SourceOffsetTracker tracker = new TestUtil.SourceOffsetTrackerImpl("1");
    FileSnapshotStore snapshotStore = Mockito.mock(FileSnapshotStore.class);

    Mockito.when(snapshotStore.getSnapshotStatus(PIPELINE_NAME, REVISION)).thenReturn(new SnapshotStatus(false, false));
    BlockingQueue<Object> productionObserveRequests = new ArrayBlockingQueue<>(100, true /*FIFO*/);
    ProductionPipelineRunner runner = new ProductionPipelineRunner(runtimeInfo, snapshotStore, deliveryGuarantee,
      PIPELINE_NAME, REVISION, new FilePipelineStoreTask(info, new Configuration()), productionObserveRequests,
      new Configuration());
    ProductionPipeline pipeline = new ProductionPipelineBuilder(MockStages.createStageLibrary(), "name",
        MockStages.createPipelineConfigurationSourceProcessorTarget()).build(runner, tracker, null);
    manager.getStateTracker().register(PIPELINE_NAME, REVISION);
    manager.getStateTracker().setState(PIPELINE_NAME, REVISION, State.STOPPED, null);

    if(capturenextBatch) {
      runner.captureNextBatch(1);
    }

    return pipeline;
  }

}
