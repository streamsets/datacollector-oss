/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseProcessor;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;

public class TestProcessorRunner {

  public static class DummyProcessorEmptyBatch extends BaseProcessor {

    @Override
    public void process(Batch batch, BatchMaker batchMaker) throws StageException {
      Assert.assertNotNull(batch.getSourceOffset());
      Iterator<Record> it = batch.getRecords();
      Assert.assertFalse(it.hasNext());
    }

  }

  public static class DummyProcessor extends BaseProcessor {

    @Override
    public void process(Batch batch, BatchMaker batchMaker) throws StageException {
      Assert.assertNotNull(batch.getSourceOffset());
      Iterator<Record> it = batch.getRecords();
      Assert.assertTrue(it.hasNext());
      Record record = it.next();
      Assert.assertNotNull(record);
      Assert.assertFalse(it.hasNext());
      batchMaker.addRecord(record);
    }

  }

  @Test(expected = RuntimeException.class)
  public void testBuilderNoOutput() throws Exception {
    DummyProcessor stage = new DummyProcessor();
    ProcessorRunner.Builder builder = new ProcessorRunner.Builder(stage);
    builder.build();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInitProcessDestroy() throws Exception {
    DummyProcessorEmptyBatch stage = new DummyProcessorEmptyBatch();
    ProcessorRunner.Builder builder = new ProcessorRunner.Builder(stage).addOutputLane("a");
    ProcessorRunner runner = builder.build();
    try {
      runner.runInit();
      runner.runProcess(Collections.EMPTY_LIST);
    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = RuntimeException.class)
  @SuppressWarnings("unchecked")
  public void testInvalidProcess1() throws Exception {
    DummyProcessor stage = new DummyProcessor();
    ProcessorRunner.Builder builder = new ProcessorRunner.Builder(stage).addOutputLane("a");
    ProcessorRunner runner = builder.build();
    try {
      runner.runProcess(Collections.EMPTY_LIST);
    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = RuntimeException.class)
  @SuppressWarnings("unchecked")
  public void testInvalidProcess2() throws Exception {
    DummyProcessor stage = new DummyProcessor();
    ProcessorRunner.Builder builder = new ProcessorRunner.Builder(stage).addOutputLane("a");
    ProcessorRunner runner = builder.build();
    runner.runInit();
    runner.runDestroy();
    runner.runProcess(Collections.EMPTY_LIST);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProcessEmptyBatch() throws Exception {
    DummyProcessorEmptyBatch stage = new DummyProcessorEmptyBatch();
    ProcessorRunner.Builder builder = new ProcessorRunner.Builder(stage).addOutputLane("a");
    ProcessorRunner runner = builder.build();
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(Collections.EMPTY_LIST);
      Assert.assertNotNull(output);
      Assert.assertNotNull(output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().size());
      Assert.assertTrue(output.getRecords().containsKey("a"));
      Assert.assertTrue(output.getRecords().get("a").isEmpty());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProcessNonEmptyBatch() throws Exception {
    DummyProcessor stage = new DummyProcessor();
    ProcessorRunner.Builder builder = new ProcessorRunner.Builder(stage).addOutputLane("a");
    ProcessorRunner runner = builder.build();
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(ImmutableList.of(RecordCreator.create()));
      Assert.assertNotNull(output);
      Assert.assertNotNull(output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().size());
      Assert.assertTrue(output.getRecords().containsKey("a"));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Assert.assertNotNull(output.getRecords().get("a").get(0));
    } finally {
      runner.runDestroy();
    }
  }

}
