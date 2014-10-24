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

import com.streamsets.pipeline.api.ErrorId;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Record;

import java.util.Iterator;

public abstract class SingleLaneProcessor extends BaseProcessor {

  public enum Error implements ErrorId {
    // We have an trailing whitespace for testing purposes
    INPUT_LANE_ERROR("There should be 1 input lane but there are '{}' "),
    OUTPUT_LANE_ERROR("There should be 1 output lane but there are '{}' ");

    private String msg;

    Error(String msg) {
      this.msg = msg;
    }

    @Override
    public String getMessageTemplate() {
      return msg;
    }
  }

  public interface SingleLaneBatch {
    public Iterator<Record> getRecords();
  }

  public interface SingleLaneBatchMaker {
    public void addRecord(Record record);
  }

  private String inputLane;
  private String outputLane;

  @Override
  protected void init() throws StageException {
    if (getContext().getInputLanes().size() != 1) {
      throw new StageException(Error.INPUT_LANE_ERROR, getContext().getInputLanes().size());
    }
    if (getContext().getOutputLanes().size() != 1) {
      throw new StageException(Error.OUTPUT_LANE_ERROR, getContext().getOutputLanes().size());
    }
    inputLane = getContext().getInputLanes().iterator().next();
    outputLane = getContext().getOutputLanes().iterator().next();
  }

  @Override
  public final void process(final com.streamsets.pipeline.api.Batch batch,
      final com.streamsets.pipeline.api.BatchMaker batchMaker) throws StageException {
    SingleLaneBatch slBatch = new SingleLaneBatch() {
      @Override
      public Iterator<Record> getRecords() {
        return batch.getRecords(inputLane);
      }
    };
    SingleLaneBatchMaker slBatchMaker = new SingleLaneBatchMaker() {
      @Override
      public void addRecord(Record record) {
        batchMaker.addRecord(record, outputLane);
      }
    };
    process(slBatch, slBatchMaker);
  }

  public abstract void process(SingleLaneBatch singleLaneBatch, SingleLaneBatchMaker singleLaneBatchMaker)
      throws StageException;

}
