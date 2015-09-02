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

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestRecordProcessor {

  @Test
  public void testProcessor() throws Exception {
    Record record1 = Mockito.mock(Record.class);
    Record record2 = Mockito.mock(Record.class);
    Batch batch = Mockito.mock(Batch.class);
    Mockito.when(batch.getRecords()).thenReturn(ImmutableSet.of(record1, record2).iterator());
    final BatchMaker batchMaker = Mockito.mock(BatchMaker.class);

    final List<Record> got = new ArrayList<Record>();
    final boolean[] emptyBatch = new boolean[1];

    Processor processor = new RecordProcessor() {
      @Override
      protected void process(Record record, BatchMaker bm) throws StageException {
        Assert.assertEquals(batchMaker, bm);
        got.add(record);
      }

      @Override
      protected void emptyBatch(BatchMaker batchMaker) throws StageException {
        emptyBatch[0] = true;
      }

    };
    processor.process(batch, batchMaker);

    Assert.assertEquals(2, got.size());
    Assert.assertEquals(record1, got.get(0));
    Assert.assertEquals(record2, got.get(1));
    Assert.assertFalse(emptyBatch[0]);

    //empty batch
    got.clear();
    Mockito.when(batch.getRecords()).thenReturn(Collections.<Record>emptySet().iterator());
    processor.process(batch, batchMaker);
    Assert.assertTrue(got.isEmpty());
    Assert.assertTrue(emptyBatch[0]);
  }

  public enum ERROR implements ErrorCode {
    ERR;

    @Override
    public String getCode() {
      return ERR.name();
    }

    @Override
    public String getMessage() {
      return "error";
    }

  }

  @Test
  public void testOnErrorHandlingDiscard() throws Exception {
    final Record record1 = Mockito.mock(Record.class);
    final Record record2 = Mockito.mock(Record.class);
    Batch batch = Mockito.mock(Batch.class);
    Mockito.when(batch.getRecords()).thenReturn(ImmutableSet.of(record1, record2).iterator());
    final BatchMaker batchMaker = Mockito.mock(BatchMaker.class);

    final List<Record> got = new ArrayList<Record>();

    Processor processor = new RecordProcessor() {
      @Override
      protected void process(Record record, BatchMaker bm) throws StageException {
        if (record == record1) {
          got.add(record);
        } else {
          throw new OnRecordErrorException(ERROR.ERR);
        }
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Processor.Context context = Mockito.mock(Processor.Context.class);
    Mockito.when(context.getOnErrorRecord()).thenReturn(OnRecordError.DISCARD);
    processor.init(info, context);
    processor.process(batch, batchMaker);
    processor.destroy();
    Assert.assertEquals(1, got.size());
    Assert.assertEquals(record1, got.get(0));
    Mockito.verify(context, Mockito.never()).toError((Record) Mockito.any(), (Exception) Mockito.any());
  }

  @Test
  public void testOnErrorHandlingToError() throws Exception {
    final Record record1 = Mockito.mock(Record.class);
    final Record record2 = Mockito.mock(Record.class);
    Batch batch = Mockito.mock(Batch.class);
    Mockito.when(batch.getRecords()).thenReturn(ImmutableSet.of(record1, record2).iterator());
    final BatchMaker batchMaker = Mockito.mock(BatchMaker.class);

    final List<Record> got = new ArrayList<Record>();

    Processor processor = new RecordProcessor() {
      @Override
      protected void process(Record record, BatchMaker bm) throws StageException {
        if (record == record1) {
          got.add(record);
        } else {
          throw new OnRecordErrorException(ERROR.ERR);
        }
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Processor.Context context = Mockito.mock(Processor.Context.class);
    Mockito.when(context.getOnErrorRecord()).thenReturn(OnRecordError.TO_ERROR);
    processor.init(info, context);
    processor.process(batch, batchMaker);
    processor.destroy();
    Assert.assertEquals(1, got.size());
    Assert.assertEquals(record1, got.get(0));
    ArgumentCaptor<Record> captor = ArgumentCaptor.forClass(Record.class);
    Mockito.verify(context, Mockito.times(1)).toError(captor.capture(), (Exception) Mockito.any());
    Assert.assertEquals(record2, captor.getValue());
  }

  @Test(expected = OnRecordErrorException.class)
  public void testOnErrorHandlingStopPipeline() throws Exception {
    final Record record1 = Mockito.mock(Record.class);
    final Record record2 = Mockito.mock(Record.class);
    Batch batch = Mockito.mock(Batch.class);
    Mockito.when(batch.getRecords()).thenReturn(ImmutableSet.of(record1, record2).iterator());
    final BatchMaker batchMaker = Mockito.mock(BatchMaker.class);

    final List<Record> got = new ArrayList<Record>();

    Processor processor = new RecordProcessor() {
      @Override
      protected void process(Record record, BatchMaker bm) throws StageException {
        if (record == record1) {
          got.add(record);
        } else {
          throw new OnRecordErrorException(ERROR.ERR);
        }
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Processor.Context context = Mockito.mock(Processor.Context.class);
    Mockito.when(context.getOnErrorRecord()).thenReturn(OnRecordError.STOP_PIPELINE);
    processor.init(info, context);
    processor.process(batch, batchMaker);
  }

  private void testOnErrorHandlingOtherException(OnRecordError onRecordError) throws Exception {
    final Record record1 = Mockito.mock(Record.class);
    final Record record2 = Mockito.mock(Record.class);
    Batch batch = Mockito.mock(Batch.class);
    Mockito.when(batch.getRecords()).thenReturn(ImmutableSet.of(record1, record2).iterator());
    final BatchMaker batchMaker = Mockito.mock(BatchMaker.class);

    final List<Record> got = new ArrayList<Record>();

    Processor processor = new RecordProcessor() {
      @Override
      protected void process(Record record, BatchMaker bm) throws StageException {
        throw new RuntimeException();
      }
    };

    Stage.Info info = Mockito.mock(Stage.Info.class);
    Processor.Context context = Mockito.mock(Processor.Context.class);
    Mockito.when(context.getOnErrorRecord()).thenReturn(onRecordError);
    processor.init(info, context);
    processor.process(batch, batchMaker);
  }

  @Test(expected = Exception.class)
  public void testOnErrorHandlingDiscardOtherException() throws Exception {
    testOnErrorHandlingOtherException(OnRecordError.DISCARD);
  }

  @Test(expected = Exception.class)
  public void testOnErrorHandlingToErrorOtherException() throws Exception {
    testOnErrorHandlingOtherException(OnRecordError.TO_ERROR);
  }

  @Test(expected = Exception.class)
  public void testOnErrorHandlingStopPipelineOtherException() throws Exception {
    testOnErrorHandlingOtherException(OnRecordError.STOP_PIPELINE);
  }

}
