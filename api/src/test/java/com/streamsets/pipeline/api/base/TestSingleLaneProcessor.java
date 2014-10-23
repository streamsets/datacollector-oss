/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
  public void testError() {
    Assert.assertNotNull(SingleLaneProcessor.Error.INPUT_LANE_ERROR.getMessageTemplate());
  }

  @Test(expected = StageException.class)
  @SuppressWarnings("unchecked")
  public void testInvalidConfig1() throws Exception {

    Processor processor = new SingleLaneProcessor() {
      @Override
      public void process(SingleLaneBatch singleLaneBatch, SingleLaneBatchMaker singleLaneBatchMaker) throws
          StageException {
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Processor.Context context = Mockito.mock(Processor.Context.class);
    Mockito.when(context.getInputLanes()).thenReturn(Collections.EMPTY_SET);
    Mockito.when(context.getOutputLanes()).thenReturn(ImmutableSet.of("l2"));
    processor.init(info, context);
  }

  @Test(expected = StageException.class)
  public void testInvalidConfig2() throws Exception {

    Processor processor = new SingleLaneProcessor() {
      @Override
      public void process(SingleLaneBatch singleLaneBatch, SingleLaneBatchMaker singleLaneBatchMaker) throws
          StageException {
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Processor.Context context = Mockito.mock(Processor.Context.class);
    Mockito.when(context.getInputLanes()).thenReturn(ImmutableSet.of("l1", "l2"));
    Mockito.when(context.getOutputLanes()).thenReturn(ImmutableSet.of("l3"));
    processor.init(info, context);
  }

  @Test(expected = StageException.class)
  @SuppressWarnings("unchecked")
  public void testInvalidConfig3() throws Exception {

    Processor processor = new SingleLaneProcessor() {
      @Override
      public void process(SingleLaneBatch singleLaneBatch, SingleLaneBatchMaker singleLaneBatchMaker) throws
          StageException {
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Processor.Context context = Mockito.mock(Processor.Context.class);
    Mockito.when(context.getInputLanes()).thenReturn(ImmutableSet.of("l1"));
    Mockito.when(context.getOutputLanes()).thenReturn(Collections.EMPTY_SET);
    processor.init(info, context);
  }

  @Test(expected = StageException.class)
  public void testInvalidConfig4() throws Exception {

    Processor processor = new SingleLaneProcessor() {
      @Override
      public void process(SingleLaneBatch singleLaneBatch, SingleLaneBatchMaker singleLaneBatchMaker) throws
          StageException {
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Processor.Context context = Mockito.mock(Processor.Context.class);
    Mockito.when(context.getInputLanes()).thenReturn(ImmutableSet.of("l1"));
    Mockito.when(context.getOutputLanes()).thenReturn(ImmutableSet.of("l2", "l3"));
    processor.init(info, context);
  }

  @Test
  public void testProcessor() throws Exception {
    Record record1 = Mockito.mock(Record.class);
    Record record2 = Mockito.mock(Record.class);
    Batch batch = Mockito.mock(Batch.class);
    Mockito.when(batch.getLanes()).thenReturn(ImmutableSet.of("l1"));
    Mockito.when(batch.getRecords(Mockito.eq("l1"))).thenReturn(ImmutableSet.of(record1, record2).iterator());
    final BatchMaker batchMaker = Mockito.mock(BatchMaker.class);

    Processor processor = new SingleLaneProcessor() {
      @Override
      public void process(SingleLaneBatch singleLaneBatch, SingleLaneBatchMaker singleLaneBatchMaker) throws
          StageException {
        Iterator<Record> it = singleLaneBatch.getRecords();
        while (it.hasNext()) {
          singleLaneBatchMaker.addRecord(it.next());
        }
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Processor.Context context = Mockito.mock(Processor.Context.class);
    Mockito.when(context.getInputLanes()).thenReturn(ImmutableSet.of("l1"));
    Mockito.when(context.getOutputLanes()).thenReturn(ImmutableSet.of("l2"));
    processor.init(info, context);

    processor.process(batch, batchMaker);
    ArgumentCaptor<Record> recordCaptor = ArgumentCaptor.forClass(Record.class);

    Mockito.verify(batch, Mockito.times(1)).getRecords(Mockito.eq("l1"));
    Mockito.verify(batchMaker, Mockito.times(2)).addRecord(recordCaptor.capture(), Mockito.eq("l2"));
    Assert.assertEquals(ImmutableList.of(record1, record2), recordCaptor.getAllValues());
  }

}
