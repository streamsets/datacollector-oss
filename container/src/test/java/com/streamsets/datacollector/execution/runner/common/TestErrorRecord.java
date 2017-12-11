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
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.manager.standalone.StandaloneAndClusterPipelineManager;
import com.streamsets.datacollector.execution.runner.standalone.StandaloneRunner;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.ErrorSink;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.production.BadRecordsHandler;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.TestUtil;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseProcessor;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import dagger.ObjectGraph;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    Pipeline.class,
    BadRecordsHandler.class
})
public class TestErrorRecord {
  private static final String SOURCE_INSTANCE_NAME = "s";
  private static final String PROCESSOR_INSTANCE_NAME = "p";
  private static final String TARGET_INSTANCE_NAME = "t";

  private static final String SOURCE_FIELD_VAL = "sourceField";
  private static final String PROCESSOR_FIELD_VAL = "processorField";
  private static final String TARGET_FIELD_VAL = "targetField";

  private Runner runner;
  private Manager manager;
  private RuntimeInfo runtimeInfo;
  private PipelineStateStore pipelineStateStore;
  private CountDownLatch latch;

  @Before()
  public void setUp() throws Exception {
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, testDir.getAbsolutePath());
    MockStages.resetStageCaptures();
    ObjectGraph objectGraph = ObjectGraph.create(new TestUtil.TestPipelineManagerModule());
    pipelineStateStore = objectGraph.get(PipelineStateStore.class);
    manager = new StandaloneAndClusterPipelineManager(objectGraph);
    manager.init();
    manager.run();
    runner = manager.getRunner(TestUtil.MY_PIPELINE, "0");
    runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());
  }

  @After
  public void tearDown() {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  private void captureMockStages(final String errorStageName, final AtomicBoolean errorStageAlreadyRun) {
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        Record record = getContext().createRecord("s:s1");
        record.set(Field.create(SOURCE_FIELD_VAL));
        if (errorStageName.equals(SOURCE_INSTANCE_NAME) && !errorStageAlreadyRun.get()) {
          getContext().toError(record, SOURCE_INSTANCE_NAME + "error");
          errorStageAlreadyRun.set(true);
        } else {
          batchMaker.addRecord(record);
        }
        return "";
      }
    });

    MockStages.setProcessorCapture(new BaseProcessor() {
      @Override
      public void process(Batch batch, BatchMaker batchMaker) throws StageException {
        Record record = getContext().createRecord("s:s1");
        record.set(Field.create(PROCESSOR_FIELD_VAL) );
        if (errorStageName.equals(PROCESSOR_INSTANCE_NAME) && !errorStageAlreadyRun.get()) {
          getContext().toError(record, PROCESSOR_INSTANCE_NAME + "error");
          errorStageAlreadyRun.set(true);
        } else {
          batchMaker.addRecord(record);
        }
      }
    });

    MockStages.setTargetCapture(new BaseTarget() {
      @Override
      public void write(Batch batch) throws StageException {
        Record record = getContext().createRecord("s:s1");
        record.set(Field.create(TARGET_FIELD_VAL));
        if (errorStageName.equals(TARGET_INSTANCE_NAME) && !errorStageAlreadyRun.get()) {
          getContext().toError(record, TARGET_INSTANCE_NAME + "error");
          errorStageAlreadyRun.set(true);
        }
        latch.countDown();
      }
    });
  }

  private void checkErrorRecords(
      List<Record> errrorRecords,
      String errorStage,
      String expectedFieldValue,
      boolean checkHeader
  ) {
    Assert.assertEquals("There should be only one error record", errrorRecords.size(), 1);
    RecordImpl errorRecord = (RecordImpl) errrorRecords.get(0);
    Assert.assertFalse("Record just created flag should be false", errorRecord.isInitialRecord());
    Assert.assertEquals(errorRecord.getHeader().getErrorStage(), errorStage);
    if (checkHeader) {
      Record sourceRecord = errorRecord.getHeader().getSourceRecord();
      Assert.assertNotNull("Source record should not be empty", sourceRecord);
      Assert.assertEquals(
          "Expected value not present in the source record field",
          expectedFieldValue,
          sourceRecord.get().getValueAsString()
      );
    }
    Assert.assertEquals(
        "Expected value not present in the field",
        expectedFieldValue,
        errorRecord.get().getValueAsString()
    );
  }

  @SuppressWarnings("unchecked")
  private void runAndCheckErrorRecords(String errorStage, String expectedFieldValue) throws Exception {
    final List<Record> badHandlerErrorRecords = new ArrayList<>();
    latch = new CountDownLatch(1);
    ProductionPipelineRunner runner = new ProductionPipelineRunner(
        TestUtil.MY_PIPELINE,
        "0",
        null,
        new Configuration(),
        runtimeInfo,
        new MetricRegistry(),
        null,
        null
    );
    runner.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);
    runner.setMemoryLimitConfiguration(new MemoryLimitConfiguration());
    runner.setObserveRequests(new ArrayBlockingQueue<>(100, true /*FIFO*/));
    runner.setOffsetTracker(new TestUtil.SourceOffsetTrackerImpl(Collections.singletonMap(Source.POLL_SOURCE_OFFSET_KEY, "1")));
    ProductionPipeline pipeline = new ProductionPipelineBuilder(
        TestUtil.MY_PIPELINE,
        "0",
        new Configuration(),
        runtimeInfo,
        MockStages.createStageLibrary(),
        runner,
        null,
        Mockito.mock(LineagePublisherTask.class)
    ).build(
        MockStages.userContext(),
        MockStages.createPipelineConfigurationSourceProcessorTarget(),
        System.currentTimeMillis()
    );
    pipeline.registerStatusListener(new TestProductionPipeline.MyStateListener());

    pipelineStateStore.saveState(
        "admin",
        TestUtil.MY_PIPELINE,
        "0",
        PipelineStatus.RUNNING,
        null,
        null,
        null,
        null,
        0,
        0
    );

    PowerMockito.replace(
        MemberMatcher.method(BadRecordsHandler.class, "handle", String.class, String.class, ErrorSink.class)
    ).with((proxy, method, args) -> {
      ErrorSink errorSink = (ErrorSink) args[2];
      for (Map.Entry<String, List<Record>> entry : errorSink.getErrorRecords().entrySet()) {
        for (Record record : entry.getValue()) {
          badHandlerErrorRecords.add(record);
        }
      }
      return null;
    });

    captureMockStages(errorStage, new AtomicBoolean(false));

    ProductionPipelineRunnable runnable =
        new ProductionPipelineRunnable(null, (StandaloneRunner) ((AsyncRunner) this.runner).getRunner(), pipeline,
            TestUtil.MY_PIPELINE, "0", Collections.<Future<?>> emptyList());
    Thread t = new Thread(runnable);
    t.start();
    latch.await();
    runnable.stop(false);
    t.join();
    Assert.assertTrue(runnable.isStopped());
    checkErrorRecords(pipeline.getErrorRecords(errorStage, 10), errorStage, expectedFieldValue, true);
    checkErrorRecords(badHandlerErrorRecords, errorStage, expectedFieldValue, false);
  }

  @Test
  public void testErrorRecordInSource() throws Exception {
    runAndCheckErrorRecords(SOURCE_INSTANCE_NAME, SOURCE_FIELD_VAL);
  }

  @Test
  public void testErrorRecordProcessor() throws Exception {
    runAndCheckErrorRecords(PROCESSOR_INSTANCE_NAME, PROCESSOR_FIELD_VAL);
  }

  @Test
  public void testErrorRecordInTarget() throws Exception {
    runAndCheckErrorRecords(TARGET_INSTANCE_NAME, TARGET_FIELD_VAL);
  }
}
