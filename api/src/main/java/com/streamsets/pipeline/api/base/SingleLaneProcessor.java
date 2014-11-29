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

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Record;

public abstract class SingleLaneProcessor extends BaseProcessor {

  private final static StageException.ID
      OUTPUT_LANE_ERROR = new StageException.ID(SingleLaneProcessor.class, "OUTPUT_LANE_ERROR",
                                                               "There should be 1 output lane but there are '{}'");

  public interface SingleLaneBatchMaker {
    public void addRecord(Record record);
  }

  private String outputLane;

  public SingleLaneProcessor() {
    setRequiresSuperInit();
  }

  @Override
  protected void init() throws StageException {
    if (getContext().getOutputLanes().size() != 1) {
      throw new StageException(OUTPUT_LANE_ERROR, getContext().getOutputLanes().size());
    }
    outputLane = getContext().getOutputLanes().iterator().next();
    setSuperInitCalled();
  }

  @Override
  public final void process(final Batch batch, final BatchMaker batchMaker) throws StageException {
    SingleLaneBatchMaker slBatchMaker = new SingleLaneBatchMaker() {
      @Override
      public void addRecord(Record record) {
        batchMaker.addRecord(record, outputLane);
      }
    };
    process(batch, slBatchMaker);
  }

  public abstract void process(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker)
      throws StageException;

}
