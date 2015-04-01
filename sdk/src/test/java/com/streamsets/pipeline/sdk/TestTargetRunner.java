/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;

public class TestTargetRunner {

  public static class DummyTargetEmptyBatch extends BaseTarget {
    public boolean write;

    @Override
    public void write(Batch batch) throws StageException {
      Assert.assertNotNull(batch.getSourceOffset());
      Iterator<Record> it = batch.getRecords();
      Assert.assertFalse(it.hasNext());
      write = true;
    }

  }

  public static class DummyTarget extends BaseTarget {
    public boolean write;

    @Override
    public void write(Batch batch) throws StageException {
      Assert.assertNotNull(batch.getSourceOffset());
      Iterator<Record> it = batch.getRecords();
      Assert.assertTrue(it.hasNext());
      Record record = it.next();
      Assert.assertNotNull(record);
      Assert.assertFalse(it.hasNext());
      write = true;
    }

  }

  @Test(expected = RuntimeException.class)
  public void testBuilderOutputLane() throws Exception {
    DummyTarget stage = new DummyTarget();
    TargetRunner.Builder builder = new TargetRunner.Builder(DummyTarget.class, stage).addOutputLane("a");
    builder.build();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInitProcessDestroy() throws Exception {
    DummyTargetEmptyBatch stage = new DummyTargetEmptyBatch();
    TargetRunner.Builder builder = new TargetRunner.Builder(DummyTarget.class, stage);
    TargetRunner runner = builder.build();
    try {
      runner.runInit();
      runner.runWrite(Collections.EMPTY_LIST);
    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = RuntimeException.class)
  @SuppressWarnings("unchecked")
  public void testInvalidProcess1() throws Exception {
    DummyTarget stage = new DummyTarget();
    TargetRunner.Builder builder = new TargetRunner.Builder(DummyTarget.class, stage);
    TargetRunner runner = builder.build();
    try {
      runner.runWrite(Collections.EMPTY_LIST);
    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = RuntimeException.class)
  @SuppressWarnings("unchecked")
  public void testInvalidProcess2() throws Exception {
    DummyTarget stage = new DummyTarget();
    TargetRunner.Builder builder = new TargetRunner.Builder(DummyTarget.class, stage);
    TargetRunner runner = builder.build();
    runner.runInit();
    runner.runDestroy();
    runner.runWrite(Collections.EMPTY_LIST);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProcessEmptyBatch() throws Exception {
    DummyTargetEmptyBatch stage = new DummyTargetEmptyBatch();
    TargetRunner.Builder builder = new TargetRunner.Builder(DummyTarget.class, stage);
    TargetRunner runner = builder.build();
    try {
      runner.runInit();
      runner.runWrite(Collections.EMPTY_LIST);
      Assert.assertTrue(stage.write);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProcessNonEmptyBatch() throws Exception {
    DummyTarget stage = new DummyTarget();
    TargetRunner.Builder builder = new TargetRunner.Builder(DummyTarget.class, stage);
    TargetRunner runner = builder.build();
    try {
      runner.runInit();
      runner.runWrite(ImmutableList.of(RecordCreator.create()));
      Assert.assertTrue(stage.write);
    } finally {
      runner.runDestroy();
    }
  }

}
