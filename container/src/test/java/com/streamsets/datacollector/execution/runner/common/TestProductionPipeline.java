/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.streamsets.datacollector.config.DeliveryGuarantee;
import com.streamsets.datacollector.config.MemoryLimitConfiguration;
import com.streamsets.datacollector.config.MemoryLimitExceeded;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.execution.StateListener;
import com.streamsets.datacollector.execution.snapshot.common.SnapshotInfoImpl;
import com.streamsets.datacollector.execution.snapshot.file.FileSnapshotStore;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.memory.TestMemoryUsageCollector;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.TestUtil;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseProcessor;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
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
import java.util.Iterator;
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
    runtimeInfo = new RuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, runtimeInfoMetrics,
                                  Arrays.asList(getClass().getClassLoader()));
    runtimeInfo.init();
    memoryLimit = new MemoryLimitConfiguration();
    MetricsConfigurator.registerJmxMetrics(runtimeInfoMetrics);
  }

  @Test
  public void testStopPipeline() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_LEAST_ONCE, false, false);
    pipeline.stop();
    Assert.assertTrue(pipeline.wasStopped());

  }

  @Test
  public void testGetCommittedOffset() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_LEAST_ONCE, false, false);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();

    //The source returns null offset the first time.
    Assert.assertEquals(null, pipeline.getCommittedOffset());
  }

  @Test
  public void testProductionRunnerOffsetAPIs() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_LEAST_ONCE, false, false);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();

    //The source returns null offset the first time.
    Assert.assertEquals("1", pipeline.getPipeline().getRunner().getSourceOffset());
    Assert.assertEquals(null, pipeline.getPipeline().getRunner().getNewSourceOffset());

  }

  @Test
  public void testProductionRunAtLeastOnce() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_LEAST_ONCE, true, false);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();

    //The source returns null offset the first time.
    Assert.assertNull(pipeline.getCommittedOffset());

  }

  @Test
  public void testProductionRunAtMostOnce() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, false);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();
    //The source returns null offset the first time.
    Assert.assertNull(pipeline.getCommittedOffset());
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
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true,
      true/*source is committer*/);
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
        if (name.startsWith(MetricsConfigurator.JMX_PREFIX)) {
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
      if (name.startsWith(MetricsConfigurator.JMX_PREFIX)) {
        Assert.fail();
      }
    }
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, true);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();
    for (String name : runtimeInfoMetrics.getNames()) {
      if (name.startsWith(MetricsConfigurator.JMX_PREFIX)) {
        Assert.fail();
      }
    }
  }

  @Test
  public void testProductionRunWithSourceOffsetTracker() throws Exception {
    SourceOffsetTrackerCapture capture = new SourceOffsetTrackerCapture();
    MockStages.setSourceCapture(capture);
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true,
      false/*source not committer*/);
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
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true,
      false/*source not committer*/);
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
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, true);
    pipeline.registerStatusListener(new MyStateListener());
    pipeline.run();
    Assert.assertFalse(capture.isPreview);
  }

  private ProductionPipeline createProductionPipeline(DeliveryGuarantee deliveryGuarantee, boolean captureNextBatch,
    boolean sourceOffsetCommitter) throws Exception {
    return createProductionPipeline(deliveryGuarantee, captureNextBatch, -1L, sourceOffsetCommitter);
  }


  private ProductionPipeline createProductionPipeline(DeliveryGuarantee deliveryGuarantee, boolean captureNextBatch, long rateLimit,
    boolean sourceOffsetCommitter) throws Exception {
    SourceOffsetTracker tracker = new TestUtil.SourceOffsetTrackerImpl("1");
    SnapshotStore snapshotStore = Mockito.mock(FileSnapshotStore.class);

    Mockito.when(snapshotStore.getInfo(PIPELINE_NAME, REVISION, SNAPSHOT_NAME)).thenReturn(
        new SnapshotInfoImpl("user", "SNAPSHOT_NAME", "SNAPSHOT LABEL", PIPELINE_NAME, REVISION,
            System.currentTimeMillis(), false));
    BlockingQueue<Object> productionObserveRequests = new ArrayBlockingQueue<>(100, true /* FIFO */);
    Configuration config = new Configuration();
    config.set("monitor.memory", true);
    ProductionPipelineRunner runner =
        new ProductionPipelineRunner(PIPELINE_NAME, REVISION, config, runtimeInfo, new MetricRegistry(), snapshotStore,
            null);
    runner.setObserveRequests(productionObserveRequests);
    runner.setMemoryLimitConfiguration(memoryLimit);
    runner.setDeliveryGuarantee(deliveryGuarantee);
    if (rateLimit > 0) {
      runner.setRateLimit(rateLimit);
    }
    PipelineConfiguration pConf =
        (sourceOffsetCommitter) ? MockStages.createPipelineConfigurationSourceOffsetCommitterProcessorTarget()
            : MockStages.createPipelineConfigurationSourceProcessorTarget();

    ProductionPipeline pipeline =
        new ProductionPipelineBuilder(PIPELINE_NAME, REVISION, config, runtimeInfo, MockStages.createStageLibrary(), runner, null)
            .build(pConf);
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
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, true);
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
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, true);
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

    final ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, 10L,
        false/*source not committer*/);
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
}
