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
package com.streamsets.pipeline.stage.processor.spark;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.processor.spark.cluster.ClusterExecutorSparkProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;

import static com.streamsets.pipeline.stage.processor.spark.Errors.SPARK_00;
import static com.streamsets.pipeline.stage.processor.spark.Errors.SPARK_01;
import static com.streamsets.pipeline.stage.processor.spark.Errors.SPARK_02;
import static com.streamsets.pipeline.stage.processor.spark.Errors.SPARK_05;

public class TestSparkProcessor {

  private static final String LANE = "spark";
  private static final String INSERTED_CONSTANT = "testing";
  private static final String INCREMENT = "1000";
  private final Class<? extends DProcessor> DProcessorClass = SparkDProcessor.class;

  private SparkProcessorConfigBean getConfigBean() {
    SparkProcessorConfigBean configBean = new SparkProcessorConfigBean();
    configBean.preprocessMethodArgs = ImmutableList.of(INCREMENT, INSERTED_CONSTANT);
    configBean.appName = "Test App";
    configBean.threadCount = 1;
    configBean.transformerClass = NoErrorTransformer.class.getCanonicalName();
    return configBean;
  }

  @Test
  public void testExecutionModes() throws Exception {
    final OnRecordError onRecordError = OnRecordError.STOP_PIPELINE;

    ProcessorRunner runner =
        new ProcessorRunner.Builder(DProcessorClass, null)
            .addOutputLane(LANE)
            .setExecutionMode(ExecutionMode.STANDALONE)
            .setOnRecordError(onRecordError)
            .build();
    Assert.assertTrue(((SparkDProcessor) runner.getStage()).createProcessor() instanceof DelegatingSparkProcessor);

    runner =
        new ProcessorRunner.Builder(DProcessorClass, new DelegatingSparkProcessor(getConfigBean(), new Semaphore(0)))
            .addOutputLane(LANE)
            .setExecutionMode(ExecutionMode.STANDALONE)
            .setOnRecordError(onRecordError)
            .build();

    DelegatingSparkProcessor sparkProcessor = new DelegatingSparkProcessor(getConfigBean(), new Semaphore(0));
    try {
      sparkProcessor.init(runner.getInfo(), (Processor.Context) runner.getContext());
      Assert.assertTrue(sparkProcessor.getUnderlyingProcessor() instanceof StandaloneSparkProcessor);
    } finally {
      sparkProcessor.destroy();
    }

    runner = new ProcessorRunner.Builder(DProcessorClass, new DelegatingSparkProcessor(getConfigBean(), new Semaphore(0)))
        .addOutputLane(LANE)
        .setOnRecordError(onRecordError)
        .setExecutionMode(ExecutionMode.CLUSTER_MESOS_STREAMING)
        .build();

    sparkProcessor = new DelegatingSparkProcessor(getConfigBean(), new Semaphore(0));
    try {
      sparkProcessor.init(runner.getInfo(), (Processor.Context) runner.getContext());
      Assert.assertTrue(sparkProcessor.getUnderlyingProcessor() instanceof ClusterExecutorSparkProcessor);
      sparkProcessor.destroy();
    } finally {
      sparkProcessor.destroy();
    }

    runner = new ProcessorRunner.Builder(DProcessorClass, new DelegatingSparkProcessor(getConfigBean(), new Semaphore(0)))
        .addOutputLane(LANE)
        .setOnRecordError(onRecordError)
        .setExecutionMode(ExecutionMode.CLUSTER_YARN_STREAMING)
        .build();

    sparkProcessor = new DelegatingSparkProcessor(getConfigBean(), new Semaphore(0));
    try {
      sparkProcessor.init(runner.getInfo(), (Processor.Context) runner.getContext());
      Assert.assertTrue(sparkProcessor.getUnderlyingProcessor() instanceof ClusterExecutorSparkProcessor);
      sparkProcessor.destroy();
    } finally {
      sparkProcessor.destroy();
    }
  }

  @Test
  public void testSuccess() throws Exception {
    final OnRecordError onRecordError = OnRecordError.STOP_PIPELINE;
    StandaloneSparkProcessor processor = new StandaloneSparkProcessor(getConfigBean());

    List<Record> records = new ArrayList<>(100);
    for (int i = 0; i < 100; i++) {
      Record r = RecordCreator.create();
      records.add(r);
      r.set(Field.create(new HashMap<String, Field>()));
      r.set(NoErrorTransformer.VALUE_PATH, Field.create(i));
    }

    ProcessorRunner runner = new ProcessorRunner.Builder(DProcessorClass, processor)
        .addOutputLane(LANE).setOnRecordError(onRecordError).build();

    try {
      runner.runInit();

      List<Record> output = runner.runProcess(records).getRecords().get(LANE);

      int increment = Integer.parseInt(INCREMENT);
      for (int i = 0; i < 100; i++) {
        Assert.assertEquals(i + increment, output.get(i).get(NoErrorTransformer.MAPPED).getValueAsInteger());
        Assert.assertEquals(INSERTED_CONSTANT, output.get(i).get(NoErrorTransformer.CONSTANT).getValueAsString());
        Assert.assertEquals(records.get(i).getHeader(), output.get(i).getHeader());
      }

    } catch(Exception ex) {
      Assert.fail(ex.getMessage());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testError() throws Exception {
    final OnRecordError onRecordError = OnRecordError.TO_ERROR;

    SparkProcessorConfigBean configBean = getConfigBean();
    configBean.transformerClass = OnlyErrorTransformer.class.getCanonicalName();

    StandaloneSparkProcessor processor = new StandaloneSparkProcessor(configBean);

    ProcessorRunner runner = new ProcessorRunner.Builder(DProcessorClass, processor)
        .addOutputLane(LANE).setOnRecordError(onRecordError).build();

    Random random = new Random();
    List<Record> records = new ArrayList<>(100);
    for (int i = 0; i < 100; i++) {
      Record r = RecordCreator.create();
      records.add(r);
      r.set(Field.create(new HashMap<String, Field>()));
      r.set(OnlyErrorTransformer.ERROR_PATH, Field.create(String.valueOf(random.nextLong())));
    }

    try {
      runner.runInit();
      runner.runProcess(records);
      List<Record> errors = runner.getErrorRecords();

      for (int i = 0; i < 100; i++) {
        Assert.assertEquals(records.get(i).get(), errors.get(i).get());
        verifyHeaders(records.get(i).getHeader(), errors.get(i).getHeader());
      }

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testSuccessError() throws Exception {
    final OnRecordError onRecordError = OnRecordError.TO_ERROR;

    SparkProcessorConfigBean configBean = getConfigBean();
    configBean.transformerClass = HalfHalfTransformer.class.getCanonicalName();

    StandaloneSparkProcessor processor = new StandaloneSparkProcessor(configBean);

    ProcessorRunner runner = new ProcessorRunner.Builder(DProcessorClass, processor)
        .addOutputLane(LANE).setOnRecordError(onRecordError).build();

    Random random = new Random();
    List<Record> records = new ArrayList<>(100);
    for (int i = 0; i < 100; i++) {
      Record r = RecordCreator.create();
      records.add(r);
      r.set(Field.create(new HashMap<String, Field>()));
      r.set(OnlyErrorTransformer.ERROR_PATH, Field.create(String.valueOf(random.nextLong())));
    }

    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(records);

      List<Record> results = output.getRecords().get(LANE);

      for (int i = 0; i < 50; i++) {
        Assert.assertEquals(records.get(i).get(), results.get(i).get());
        Assert.assertEquals(records.get(i).getHeader(), results.get(i).getHeader());
      }

      List<Record> errors = runner.getErrorRecords();

      for (int i = 50; i < 100; i++) {
        Assert.assertEquals(records.get(i).get(), errors.get(i - 50).get());
        verifyHeaders(records.get(i).getHeader(), errors.get(i - 50).getHeader());
      }

    } finally {
      runner.runDestroy();
    }
  }

  private void verifyHeaders(Record.Header input, Record.Header output) {
    for (String inputAttr : input.getAttributeNames()) {
      Assert.assertEquals(input.getAttribute(inputAttr), output.getAttribute(inputAttr));
    }
    Assert.assertEquals(input.getSourceId(), output.getSourceId());
    Assert.assertEquals(input.getStageCreator(), output.getStageCreator());
    Assert.assertEquals(input.getStagesPath(), output.getStagesPath());
    Assert.assertEquals(input.getTrackingId(), output.getTrackingId());
    Assert.assertArrayEquals(input.getRaw(), output.getRaw());
    Assert.assertEquals(input.getRawMimeType(), output.getRawMimeType());
    Assert.assertEquals(input.getPreviousTrackingId(), output.getPreviousTrackingId());
  }

  @Test
  public void testNonExistentTransformer() throws Exception {
    final OnRecordError onRecordError = OnRecordError.TO_ERROR;

    SparkProcessorConfigBean configBean = getConfigBean();
    configBean.transformerClass = "Non-Existent Class";

    StandaloneSparkProcessor processor = new StandaloneSparkProcessor(configBean);

    ProcessorRunner runner = new ProcessorRunner.Builder(DProcessorClass, processor)
        .addOutputLane(LANE).setOnRecordError(onRecordError).build();

    try {
      runner.runInit();
      Assert.fail();
    } catch (StageException ex) {
      Assert.assertTrue(ex.getMessage().contains(SPARK_01.name()));
      Assert.assertTrue(ex.getMessage().contains(Utils.format(SPARK_01.getMessage(), configBean.transformerClass)));
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testInvalidTransformer() throws Exception {
    final OnRecordError onRecordError = OnRecordError.TO_ERROR;

    SparkProcessorConfigBean configBean = getConfigBean();
    configBean.transformerClass = ClassNotImplementingSparkTransformer.class.getCanonicalName();

    StandaloneSparkProcessor processor = new StandaloneSparkProcessor(configBean);

    ProcessorRunner runner = new ProcessorRunner.Builder(DProcessorClass, processor)
        .addOutputLane(LANE).setOnRecordError(onRecordError).build();

    try {
      runner.runInit();
      Assert.fail();
    } catch (StageException ex) {
      Assert.assertTrue(ex.getMessage().contains(SPARK_00.name()));
      Assert.assertTrue(ex.getMessage().contains(Utils.format(SPARK_00.getMessage(), configBean.transformerClass)));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testTransformerInstantiationFail() throws Exception {
    final OnRecordError onRecordError = OnRecordError.TO_ERROR;

    SparkProcessorConfigBean configBean = getConfigBean();
    configBean.transformerClass = ConstructorThrowingSparkTransformer.class.getCanonicalName();

    StandaloneSparkProcessor processor = new StandaloneSparkProcessor(configBean);

    ProcessorRunner runner = new ProcessorRunner.Builder(DProcessorClass, processor)
        .addOutputLane(LANE).setOnRecordError(onRecordError).build();

    try {
      runner.runInit();
      Assert.fail();
    } catch (StageException ex) {
      Assert.assertTrue(ex.getMessage().contains(SPARK_02.name()));
      Assert.assertTrue(ex.getMessage().contains(Utils.format(SPARK_02.getMessage(), configBean.transformerClass, "java.lang.IllegalStateException")));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testTransformerInitializationFail() throws Exception {
    final OnRecordError onRecordError = OnRecordError.TO_ERROR;

    SparkProcessorConfigBean configBean = getConfigBean();
    configBean.transformerClass = InitThrowingSparkTransformer.class.getCanonicalName();

    StandaloneSparkProcessor processor = new StandaloneSparkProcessor(configBean);

    ProcessorRunner runner = new ProcessorRunner.Builder(DProcessorClass, processor)
        .addOutputLane(LANE).setOnRecordError(onRecordError).build();

    try {
      runner.runInit();
      Assert.fail();
    } catch (StageException ex) {
      Assert.assertTrue(ex.getMessage().contains(SPARK_05.name()));
      Assert.assertTrue(ex.getMessage().contains(Utils.format(SPARK_05.getMessage(), configBean.transformerClass, "java.lang.IllegalStateException : Error")));
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testMethodCalls() throws Exception {
    final OnRecordError onRecordError = OnRecordError.TO_ERROR;

    SparkProcessorConfigBean configBean = getConfigBean();
    configBean.transformerClass = MethodCallCountingTransformer.class.getCanonicalName();

    StandaloneSparkProcessor processor = new StandaloneSparkProcessor(configBean);

    Random random = new Random();
    List<Record> records = new ArrayList<>(100);
    for (int i = 0; i < 100; i++) {
      Record r = RecordCreator.create();
      records.add(r);
      r.set(Field.create(new HashMap<String, Field>()));
      r.set(OnlyErrorTransformer.ERROR_PATH, Field.create(String.valueOf(random.nextLong())));
    }

    ProcessorRunner runner = new ProcessorRunner.Builder(DProcessorClass, processor)
        .addOutputLane(LANE).setOnRecordError(onRecordError).build();

    try {
      runner.runInit();

      for (int i = 0; i < 10; i++) {
        runner.runProcess(records);
      }
    } finally {
      runner.runDestroy();
    }

    Assert.assertEquals(MethodCallCountingTransformer.initCallCount, 1);
    Assert.assertEquals(MethodCallCountingTransformer.transformCallCount, 10);
    Assert.assertEquals(MethodCallCountingTransformer.destroyCallCount, 1);
  }

  @Test
  public void testExceptionStrings() {

    Assert.assertEquals("java.lang.RuntimeException : Shire",
        StandaloneSparkProcessor.getExceptionString(new RuntimeException("Shire")));

    Assert.assertEquals("java.lang.RuntimeException : Baggins",
        StandaloneSparkProcessor.getExceptionString(new RuntimeException("Baggins")));

    Assert.assertEquals("java.lang.NumberFormatException : Mordor",
        StandaloneSparkProcessor.getExceptionString(new NumberFormatException("Mordor")));

    Assert.assertEquals("java.lang.IllegalStateException",
        StandaloneSparkProcessor.getExceptionString(new IllegalStateException()));
  }
}
