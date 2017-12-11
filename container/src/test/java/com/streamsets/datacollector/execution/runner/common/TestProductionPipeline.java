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

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.config.MemoryLimitConfiguration;
import com.streamsets.datacollector.config.MemoryLimitExceeded;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.execution.StateListener;
import com.streamsets.datacollector.execution.snapshot.common.SnapshotInfoImpl;
import com.streamsets.datacollector.execution.snapshot.file.FileSnapshotStore;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.datacollector.memory.TestMemoryUsageCollector;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.TestUtil;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseExecutor;
import com.streamsets.pipeline.api.base.BaseProcessor;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Exchanger;

public class TestProductionPipeline {

  private static final String PIPELINE_NAME = "myPipeline";
  private static final String REVISION = "0";
  private static final String SNAPSHOT_NAME = "snapshot";
  private MetricRegistry runtimeInfoMetrics;
  private MemoryLimitConfiguration memoryLimit;
  private RuntimeInfo runtimeInfo;

  // Private enum for this testcase to figure out which pipeline should be used for test
  private enum PipelineType {
    DEFAULT,
    OFFSET_COMMITTERS,
    EVENTS,
    PUSH_SOURCE,
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, "./target/var");
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    TestUtil.captureMockStages();
    TestMemoryUsageCollector.initalizeMemoryUtility();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  @Before
  public void setUp() {
    runtimeInfoMetrics = new MetricRegistry();
    runtimeInfo = new StandaloneRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, runtimeInfoMetrics,
                                  Arrays.asList(getClass().getClassLoader()));
    runtimeInfo.init();
    memoryLimit = new MemoryLimitConfiguration();
    MetricsConfigurator.registerJmxMetrics(runtimeInfoMetrics);

    MockStages.setSourceCapture(null);
    MockStages.setPushSourceCapture(null);
    MockStages.setProcessorCapture(null);
    MockStages.setExecutorCapture(null);
    MockStages.setTargetCapture(null);
  }

  @Test
  public void testStopPipeline() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_LEAST_ONCE, false, PipelineType.DEFAULT);
    pipeline.stop();
    Assert.assertTrue(pipeline.wasStopped());

  }

  @Test
  public void testGetCommittedOffset() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_LEAST_ONCE, false, PipelineType.DEFAULT);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();

    Assert.assertTrue(pipeline.getCommittedOffsets().isEmpty());
  }

  @Test
  public void testProductionRunAtLeastOnce() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_LEAST_ONCE, true, PipelineType.DEFAULT);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();

    Assert.assertTrue(pipeline.getCommittedOffsets().isEmpty());
  }

  @Test
  public void testProductionRunAtMostOnce() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, PipelineType.DEFAULT);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();
    Assert.assertTrue(pipeline.getCommittedOffsets().isEmpty());
  }

  private static class SourceOffsetCommitterCapture extends BaseSource implements OffsetCommitter {
    public int count;
    public String offset;
    public long lastBatchTime;

    @Override
    public void commit(String offset) throws StageException {
      this.offset = offset;
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      lastBatchTime = getContext().getLastBatchTime();
      return (count++ == 0) ? "x" : null;
    }

  }

  private static class SourceOffsetTrackerCapture extends BaseSource {
    public int count;
    public String offset;
    public long lastBatchTime;

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      lastBatchTime = getContext().getLastBatchTime();
      return (count++ == 0) ? "x" : null;
    }

  }

  @Test
  public void testProductionRunWithSourceOffsetCommitter() throws Exception {
    SourceOffsetCommitterCapture capture = new SourceOffsetCommitterCapture();
    MockStages.setSourceCapture(capture);
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, PipelineType.OFFSET_COMMITTERS);
    pipeline.registerStatusListener(new MyStateListener());
    long startTime = System.currentTimeMillis();
    //Need sleep because the file system could truncate the time to the last second.
    Thread.sleep(1000);
    pipeline.run();
    //Need sleep because the file system could truncate the time to the last second.
    Thread.sleep(1000);
    long endTime = System.currentTimeMillis();
    Assert.assertEquals(null, capture.offset);
    Assert.assertTrue(capture.lastBatchTime > startTime);
    Assert.assertTrue(capture.lastBatchTime < endTime);
  }


  private class RuntimeInfoMetricCheckSource extends BaseSource {

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      for (String name : runtimeInfoMetrics.getNames()) {
        if (name.startsWith(MetricsConfigurator.JMX_PIPELINE_PREFIX)) {
          return null;
        }
      }
      Assert.fail();
      return null;
    }

  }

  @Test
  public void testPipelineMetricsInRuntimeMetrics() throws Exception {
    Source capture = new RuntimeInfoMetricCheckSource();
    MockStages.setSourceCapture(capture);
    for (String name : runtimeInfoMetrics.getNames()) {
      if (name.startsWith(MetricsConfigurator.JMX_PIPELINE_PREFIX)) {
        Assert.fail();
      }
    }
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, PipelineType.OFFSET_COMMITTERS);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();
    for (String name : runtimeInfoMetrics.getNames()) {
      if (name.startsWith(MetricsConfigurator.JMX_PIPELINE_PREFIX)) {
        Assert.fail();
      }
    }
  }

  @Test
  public void testProductionRunWithSourceOffsetTracker() throws Exception {
    SourceOffsetTrackerCapture capture = new SourceOffsetTrackerCapture();
    MockStages.setSourceCapture(capture);
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, PipelineType.DEFAULT);
    pipeline.registerStatusListener(new MyStateListener());
    long startTime = System.currentTimeMillis();
    //Need sleep because the file system could truncate the time to the last second.
    Thread.sleep(1000);
    pipeline.run();
    //Need sleep because the file system could truncate the time to the last second.
    Thread.sleep(1000);
    long endTime = System.currentTimeMillis();
    Assert.assertEquals(null, capture.offset);
    Assert.assertTrue(capture.lastBatchTime > startTime);
    Assert.assertTrue(capture.lastBatchTime < endTime);
  }

  @Test
  public void testMemoryLimit() throws Exception {
    memoryLimit = new MemoryLimitConfiguration(MemoryLimitExceeded.STOP_PIPELINE, 1);
    SourceOffsetTrackerCapture capture = new SourceOffsetTrackerCapture() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        try {
          Thread.sleep(10000); // sleep enough time to get
        } catch (InterruptedException e) {}
        return super.produce(lastSourceOffset, maxBatchSize, batchMaker);
      }
    };
    MockStages.setSourceCapture(capture);
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, PipelineType.DEFAULT);
    //Need sleep because the file system could truncate the time to the last second.
    pipeline.registerStatusListener(new MyStateListener());
    Thread.sleep(15000);
    try {
      pipeline.run();
      Assert.fail("Expected PipelineRuntimeException");
    } catch (PipelineRuntimeException e) {
      Assert.assertEquals(ContainerError.CONTAINER_0011, e.getErrorCode());
    }
  }

  @Test
  public void testNoRerunOnJVMError() throws Exception {
    SourceOffsetTrackerCapture capture = new SourceOffsetTrackerCapture() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        throw new OutOfMemoryError();
      }
    };
    verifyRerunScenario(capture, PipelineStatus.RUN_ERROR);
  }

  @Test
  public void testRerunOnNormalException() throws Exception {
    SourceOffsetTrackerCapture capture = new SourceOffsetTrackerCapture() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        throw new RuntimeException();
      }
    };
    verifyRerunScenario(capture, PipelineStatus.RETRY);
  }

  public void verifyRerunScenario(SourceOffsetTrackerCapture capture, PipelineStatus finalStatus) throws Exception {
    Source originalSource = MockStages.getSourceCapture();
    try {
      PersistChangesStateListener listener = new PersistChangesStateListener();
      MockStages.setSourceCapture(capture);
      ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, PipelineType.DEFAULT);
      pipeline.registerStatusListener(listener);

      boolean exceptionThrown = true;
      try {
        pipeline.run();
        exceptionThrown = false;
      } catch (Throwable t) {
        // suppress exception so that we can continue and retry.
      }

      if (!exceptionThrown) {
        Assert.fail("Expected exception thrown by the pipeline");
      }

      Assert.assertEquals(finalStatus, listener.statuses.get(listener.statuses.size() - 1));
    } finally {
      MockStages.setSourceCapture(originalSource);
    }
  }

  public static class PreviewCheckSource extends BaseSource {
    public boolean isPreview;

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      isPreview  = getContext().isPreview();
      return null;
    }
  }

  @Test
  public void testIsPreview() throws Exception {
    PreviewCheckSource capture = new PreviewCheckSource();
    MockStages.setSourceCapture(capture);
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, PipelineType.OFFSET_COMMITTERS);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();
    Assert.assertFalse(capture.isPreview);
  }

  @Test
  public void testErrorNotificationOnValidationFailure() throws Exception {
    ProductionPipeline prodPipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE,
        true,
        PipelineType.OFFSET_COMMITTERS
    );
    prodPipeline.registerStatusListener(new MyStateListener());
    Pipeline actual = prodPipeline.getPipeline();
    Pipeline pipeline = Mockito.spy(actual);
    Issue issue = Mockito.mock(Issue.class);
    Mockito.doReturn(Arrays.asList(issue)).when(pipeline).init(true);
    ProductionPipeline mockProdPipeline = Mockito.spy(prodPipeline);
    Mockito.doReturn(pipeline).when(mockProdPipeline).getPipeline();
    try {
      mockProdPipeline.run();
    } catch (Exception e) {
      //
    }
    Mockito.verify(pipeline).errorNotification(Mockito.any(Throwable.class));
  }


  private ProductionPipeline createProductionPipeline(DeliveryGuarantee deliveryGuarantee, boolean captureNextBatch,
    PipelineType type) throws Exception {
    return createProductionPipeline(deliveryGuarantee, captureNextBatch, -1L, type);
  }

  private ProductionPipeline createProductionPipeline(DeliveryGuarantee deliveryGuarantee, boolean captureNextBatch, long rateLimit, PipelineType type) throws Exception {
    SourceOffsetTracker tracker = new TestUtil.SourceOffsetTrackerImpl(Collections.singletonMap(Source.POLL_SOURCE_OFFSET_KEY, "1"));
    SnapshotStore snapshotStore = Mockito.mock(FileSnapshotStore.class);

    Mockito.when(snapshotStore.getInfo(PIPELINE_NAME, REVISION, SNAPSHOT_NAME)).thenReturn(
        new SnapshotInfoImpl("user", "SNAPSHOT_NAME", "SNAPSHOT LABEL", PIPELINE_NAME, REVISION,
            System.currentTimeMillis(), false, 0));
    BlockingQueue<Object> productionObserveRequests = new ArrayBlockingQueue<>(100, true /* FIFO */);
    Configuration config = new Configuration();
    config.set("monitor.memory", true);
    ProductionPipelineRunner runner = new ProductionPipelineRunner(
      PIPELINE_NAME,
      REVISION,
      null,
      config,
      runtimeInfo,
      new MetricRegistry(),
      snapshotStore,
      null
    );
    runner.setObserveRequests(productionObserveRequests);
    runner.setMemoryLimitConfiguration(memoryLimit);
    runner.setDeliveryGuarantee(deliveryGuarantee);
    if (rateLimit > 0) {
      runner.setRateLimit(rateLimit);
    }
    PipelineConfiguration pConf = null;
    switch(type) {
      case DEFAULT:
        pConf = MockStages.createPipelineConfigurationSourceProcessorTarget();
        break;
      case OFFSET_COMMITTERS:
        pConf =  MockStages.createPipelineConfigurationSourceOffsetCommitterProcessorTarget();
        break;
      case EVENTS:
        pConf =  MockStages.createPipelineConfigurationSourceTargetWithEventsProcessed();
        break;
      case PUSH_SOURCE:
        pConf =  MockStages.createPipelineConfigurationPushSourceTarget();
        break;
    }

    ProductionPipeline pipeline = new ProductionPipelineBuilder(
      PIPELINE_NAME,
      REVISION,
      config,
      runtimeInfo,
      MockStages.createStageLibrary(),
      runner,
      null,
      Mockito.mock(LineagePublisherTask.class)
    ).build(
      MockStages.userContext(),
      pConf,
      System.currentTimeMillis()
    );
    runner.setOffsetTracker(tracker);

    if (captureNextBatch) {
      runner.capture("snapshot", 1, 1);
    }

    return pipeline;
  }

  private static class SourceValidateConfigFailureCapture implements Source {
    public int count;
    public String offset;

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return (count++ == 0) ? "x" : null;
    }

    @Override
    public List<ConfigIssue> init(Info info, Context context) {
      return Arrays.asList(context.createConfigIssue(null, null, ContainerError.CONTAINER_0000));
    }

    @Override
    public void destroy() {

    }
  }


  @Test(expected = PipelineRuntimeException.class)
  public void testProductionRunWithFailedValidateConfigs() throws Exception {
    Source capture = new SourceValidateConfigFailureCapture();
    MockStages.setSourceCapture(capture);
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, PipelineType.OFFSET_COMMITTERS);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();
  }

  public enum TestErrors implements ErrorCode {
    ERROR_S, ERROR_P;

    @Override
    public String getCode() {
      return name();
    }

    @Override
    public String getMessage() {
      return "ERROR";
    }
  }

  private static class ErrorProducerSourceCapture extends BaseSource {
    public int count;
    public String offset;

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      Record record = getContext().createRecord("e" + count);
      record.set(Field.create("E" + count));
      getContext().toError(record, TestErrors.ERROR_S);
      record.set(Field.create("OK" + count));
      batchMaker.addRecord(record);
      return (++count < 2) ? "o::" + count : null;
    }

  }

  private static class ErrorStageTarget extends BaseTarget {
    public List<Record> records = new ArrayList<>();

    @Override
    public void write(Batch batch) throws StageException {
      Iterator<Record> it = batch.getRecords();
      while (it.hasNext()) {
        records.add(it.next());
      }
    }
  }

  private static class ErrorProducerProcessorCapture extends BaseProcessor {
    @Override
    public void process(Batch batch, BatchMaker batchMaker) throws StageException {
      Iterator<Record> it = batch.getRecords();
      while (it.hasNext()) {
        getContext().toError(it.next(), TestErrors.ERROR_P);
      }
    }
  }

  @Test
  public void testErrorStage() throws Exception {
    Source source = new ErrorProducerSourceCapture();
    MockStages.setSourceCapture(source);
    ErrorProducerProcessorCapture processor = new ErrorProducerProcessorCapture();
    MockStages.setProcessorCapture(processor);
    ErrorStageTarget errorStage = new ErrorStageTarget();
    MockStages.setErrorStageCapture(errorStage);
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, PipelineType.OFFSET_COMMITTERS);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();
    Assert.assertEquals(4, errorStage.records.size());
    assertErrorRecord(errorStage.records.get(0), runtimeInfo.getId(), PIPELINE_NAME, Field.create("E0"),
                      TestErrors.ERROR_S.name(), "s");
    assertErrorRecord(errorStage.records.get(1), runtimeInfo.getId(), PIPELINE_NAME, Field.create("OK0"),
                      TestErrors.ERROR_P.name(), "p");
    assertErrorRecord(errorStage.records.get(2), runtimeInfo.getId(), PIPELINE_NAME, Field.create("E1"),
                      TestErrors.ERROR_S.name(), "s");
    assertErrorRecord(errorStage.records.get(3), runtimeInfo.getId(), PIPELINE_NAME, Field.create("OK1"),
                      TestErrors.ERROR_P.name(), "p");
  }

  private void assertErrorRecord(Record record, String sdcId, String pipelineName, Field field, String errorCode,
                                 String errorStage) {
    Assert.assertEquals(sdcId, record.getHeader().getErrorDataCollectorId());
    Assert.assertEquals(pipelineName, record.getHeader().getErrorPipelineName());
    Assert.assertEquals(field, record.get());
    Assert.assertEquals(errorCode, record.getHeader().getErrorCode());
    Assert.assertEquals(errorStage, record.getHeader().getErrorStage());
    Assert.assertNotNull(record.getHeader().getErrorMessage());
    Assert.assertNotEquals(0, record.getHeader().getErrorTimestamp());
  }

  static class MyStateListener implements StateListener {
    @Override
    public void stateChanged(PipelineStatus pipelineStatus, String message, Map<String, Object> attributes)
      throws PipelineRuntimeException {
    }
  }

  static class PersistChangesStateListener implements StateListener {
    List<PipelineStatus> statuses;

    public PersistChangesStateListener() {
      statuses = new LinkedList<>();
    }

    @Override
    public void stateChanged(PipelineStatus pipelineStatus, String message, Map<String, Object> attributes)
      throws PipelineRuntimeException {
      statuses.add(pipelineStatus);
    }
  }

  private static class TestProducer extends BaseSource {
    public Integer count = 0;
    public String offset;
    public Long totalDuration = 0L;


    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      long start = System.nanoTime();
      Record record = getContext().createRecord("e" + count);
      record.set(Field.create(count));
      batchMaker.addRecord(record);
      count = count + 1;
      totalDuration += (System.nanoTime() - start);
      return "o::" + count;
    }
  }


  @Test
  public void testRateLimit() throws Exception {
    final TestProducer p = new TestProducer();
    MockStages.setSourceCapture(p);

    final ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, 10L, PipelineType.DEFAULT);
    pipeline.registerStatusListener(new MyStateListener());
    final Exchanger<Double> rate = new Exchanger<>();
    new Thread() {
      @Override
      public void run() {
        try {
          long start = System.nanoTime();
          pipeline.run();
          rate.exchange(p.count.doubleValue() * 1000 * 1000 * 1000 / (System.nanoTime() - start));
        } catch (Exception ex) {

        }
      }
    }.start();
    Thread.sleep(10000);
    pipeline.stop();
    Double rateAchieved = rate.exchange(0.0);
    // To account for the slight loss of precision, we compare the "long-ified" versions.
    Assert.assertTrue(rateAchieved.longValue() <= 10);
  }

  private static class ProduceEventOnDestroySource extends BaseSource {
    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }

    @Override
    public void destroy() {
      EventRecord event = getContext().createEventRecord("x", 1, "recordSourceId");
      event.set(Field.create("event"));
      getContext().toEvent(event);
    }
  }

  private static class CaptureExecutor extends BaseExecutor {
    List<Record> records = new ArrayList<>();

    @Override
    public void write(Batch batch) throws StageException {
      Iterator<Record> it = batch.getRecords();
      while(it.hasNext()) {
        records.add(it.next());
      }
    }
  }

  @Test
  // Verify that events generated on destroy are properly processed by other stages in the pipeline
  public void testPropagatingEventsOnDestroy() throws Exception {
    Source source = new ProduceEventOnDestroySource();
    MockStages.setSourceCapture(source);
    CaptureExecutor executor = new CaptureExecutor();
    MockStages.setExecutorCapture(executor);
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, PipelineType.EVENTS);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();

    Assert.assertEquals(1, executor.records.size());
  }

  private static class ToErrorExecutor extends BaseExecutor {

    @Override
    public List<ConfigIssue> init(Info info, Target.Context context) {
      return super.init(info, context);
    }

    @Override
    public void write(Batch batch) throws StageException {
      Iterator<Record> it = batch.getRecords();
      while(it.hasNext()) {
        getContext().toError(it.next(), "Sorry");
      }
    }
  }

  @Test
  // Verify that event record that generates error record will get properly routed to error lane
  public void testPropagatingEventErrorRecordsOnDestroy() throws Exception {
    Source source = new ProduceEventOnDestroySource();
    MockStages.setSourceCapture(source);
    ToErrorExecutor executor = new ToErrorExecutor();
    MockStages.setExecutorCapture(executor);
    ErrorStageTarget errorStage = new ErrorStageTarget();
    MockStages.setErrorStageCapture(errorStage);
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, PipelineType.EVENTS);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();

    Assert.assertEquals(1, errorStage.records.size());
  }

  /**
   * Simple PushSource implementation for testing that will end after producing one record.
   */
  private static class MPushSource extends BasePushSource {

    boolean shouldProduceError;

    @Override
    public int getNumberOfThreads() {
      return 1;
    }

    @Override
    public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
      // Conditional error generation
      if(shouldProduceError) {
        getContext().reportError("Cake is a lie");
      }

      // Produce one batch
      BatchContext batchContext = getContext().startBatch();
      Record record = getContext().createRecord("abcd");
      record.set(Field.create("text"));
      batchContext.getBatchMaker().addRecord(record);
      getContext().processBatch(batchContext);
    }
  }

  private static class CaptureTarget extends BaseTarget {
    List<Record> records = new ArrayList<>();

    @Override
    public void write(Batch batch) throws StageException {
      Iterator<Record> it = batch.getRecords();
      while(it.hasNext()) {
        records.add(it.next());
      }
    }
  }

  /**
   * Run simple PushSource origin to make sure that we can execute the origin properly.
   */
  @Test
  public void testPushSource() throws Exception {
    CaptureTarget target = new CaptureTarget();

    MockStages.setPushSourceCapture(new MPushSource());
    MockStages.setTargetCapture(target);

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, PipelineType.PUSH_SOURCE);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();

    Assert.assertEquals(1, target.records.size());
  }

  /**
   * Producing error messages should work even outside of batch context
   */
  @Test
  public void testPushSourceReportError() throws Exception {
    MPushSource source = new MPushSource();
    source.shouldProduceError = true;

    MockStages.setPushSourceCapture(source);

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, PipelineType.PUSH_SOURCE);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();

    List<ErrorMessage> errors = pipeline.getErrorMessages("s", 10);
    Assert.assertNotNull(errors);
    Assert.assertEquals(1, errors.size());
    Assert.assertTrue(Utils.format("Unexpected message: {}", errors.get(0)), errors.get(0).toString().contains("Cake is a lie"));
  }

  /**
   * Validate that exception while pipeline is executing get's properly propagated up
   */
  @Test(expected = RuntimeException.class)
  public void testPushSourcePropagatesExceptionFromExecution() throws Exception {
    MPushSource source = new MPushSource();
    MockStages.setPushSourceCapture(source);
    MockStages.setTargetCapture(new BaseTarget() {
      @Override
      public void write(Batch batch) throws StageException {
        throw new RuntimeException("End of the world!");
      }
    });

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, PipelineType.PUSH_SOURCE);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();
  }

}
