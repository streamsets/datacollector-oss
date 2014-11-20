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
package com.streamsets.pipeline.util;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.runner.SourceOffsetTracker;

import java.util.Random;
import java.util.UUID;

public class TestUtil {

  public static class SourceOffsetTrackerImpl implements SourceOffsetTracker {
    private String currentOffset;
    private String newOffset;
    private boolean finished;

    public SourceOffsetTrackerImpl(String currentOffset) {
      this.currentOffset = currentOffset;
      finished = false;
    }

    @Override
    public boolean isFinished() {
      return finished;
    }

    @Override
    public String getOffset() {
      return currentOffset;
    }

    @Override
    public void setOffset(String newOffset) {
      this.newOffset = newOffset;
    }

    @Override
    public void commitOffset() {
      currentOffset = newOffset;
      finished = (currentOffset == null);
      newOffset = null;
    }
  }


  public static void captureMockStages() {
    MockStages.setSourceCapture(new BaseSource() {
      private int recordsProducedCounter = 0;

      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        Record record = getContext().createRecord("x");
        record.setField("f", Field.create(1));
        batchMaker.addRecord(record);
        recordsProducedCounter++;
        if (recordsProducedCounter == 1) {
          recordsProducedCounter = 0;
          return null;
        }
        return "1";
      }
    });
    MockStages.setProcessorCapture(new SingleLaneRecordProcessor() {
      @Override
      protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
        record.setField("f", Field.create(2));
        batchMaker.addRecord(record);
      }
    });
    MockStages.setTargetCapture(new BaseTarget() {
      @Override
      public void write(Batch batch) throws StageException {
      }
    });
  }

  public static void captureStagesForProductionRun() {
    MockStages.setSourceCapture(new BaseSource() {

      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        maxBatchSize = (maxBatchSize > -1) ? maxBatchSize : 10;
        for (int i = 0; i < maxBatchSize; i++ ) {
          batchMaker.addRecord(createRecord(lastSourceOffset, i));
        }
        return "random";
      }

      private Record createRecord(String lastSourceOffset, int batchOffset) {
        Record record = getContext().createRecord("random:" + batchOffset);
        record.setField("name", Field.create(UUID.randomUUID().toString()));
        record.setField("time", Field.create(System.currentTimeMillis()));
        return record;
      }
    });
    MockStages.setProcessorCapture(new SingleLaneRecordProcessor() {
      @Override
      protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
        record.setField("uuid", Field.create(UUID.randomUUID().toString()));
        batchMaker.addRecord(record);
      }
    });
    MockStages.setTargetCapture(new BaseTarget() {
      @Override
      public void write(Batch batch) throws StageException {
      }
    });
  }

}
