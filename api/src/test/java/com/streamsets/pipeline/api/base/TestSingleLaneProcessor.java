/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Iterator;

public class TestSingleLaneProcessor {

  @Test
  @SuppressWarnings("unchecked")
  public void testInvalidConfig1() throws Exception {

    Processor processor = new SingleLaneProcessor() {
      @Override
      public void process(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Processor.Context context = Mockito.mock(Processor.Context.class);
    Mockito.when(context.getOutputLanes()).thenReturn(Collections.EMPTY_LIST);
    Assert.assertFalse(processor.init(info, context).isEmpty());
  }

  @Test
  public void testInvalidConfig2() throws Exception {

    Processor processor = new SingleLaneProcessor() {
      @Override
      public void process(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Processor.Context context = Mockito.mock(Processor.Context.class);
    Mockito.when(context.getOutputLanes()).thenReturn(ImmutableList.of("l2", "l3"));
    Assert.assertFalse(processor.init(info, context).isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInvalidConfigMissingSuperInit() throws Exception {

    Processor processor = new SingleLaneProcessor() {

      @Override
      protected void initX() throws StageException {
      }

      @Override
      public void process(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Processor.Context context = Mockito.mock(Processor.Context.class);
    Mockito.when(context.getOutputLanes()).thenReturn(Collections.EMPTY_LIST);
    Assert.assertFalse(processor.init(info, context).isEmpty());
  }

  @Test
  public void testProcessor() throws Exception {
    Record record1 = Mockito.mock(Record.class);
    Record record2 = Mockito.mock(Record.class);
    Batch batch = Mockito.mock(Batch.class);
    Mockito.when(batch.getRecords()).thenReturn(ImmutableSet.of(record1, record2).iterator());
    final BatchMaker batchMaker = Mockito.mock(BatchMaker.class);

    Processor processor = new SingleLaneProcessor() {

      @Override
      protected void initX() throws StageException {
        super.initX();
      }

      @Override
      public void process(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
        Iterator<Record> it = batch.getRecords();
        while (it.hasNext()) {
          singleLaneBatchMaker.addRecord(it.next());
        }
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Processor.Context context = Mockito.mock(Processor.Context.class);
    Mockito.when(context.getOutputLanes()).thenReturn(ImmutableList.of("l2"));
    processor.init(info, context);

    processor.process(batch, batchMaker);
    ArgumentCaptor<Record> recordCaptor = ArgumentCaptor.forClass(Record.class);

    Mockito.verify(batch, Mockito.times(1)).getRecords();
    Mockito.verify(batchMaker, Mockito.times(2)).addRecord(recordCaptor.capture(), Mockito.eq("l2"));
    Assert.assertEquals(ImmutableList.of(record1, record2), recordCaptor.getAllValues());
  }

}
