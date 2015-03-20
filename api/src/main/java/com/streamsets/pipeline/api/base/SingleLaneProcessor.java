/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Record;

public abstract class SingleLaneProcessor extends BaseProcessor {

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
      throw new StageException(Errors.API_00, getInfo().getInstanceName(), getContext().getOutputLanes().size());
    }
    outputLane = getContext().getOutputLanes().iterator().next();
    setSuperInitCalled();
  }

  @Override
  public void process(final Batch batch, final BatchMaker batchMaker) throws StageException {
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
