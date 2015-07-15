/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Record;

import java.util.List;

public abstract class SingleLaneProcessor extends BaseProcessor {

  public interface SingleLaneBatchMaker {
    public void addRecord(Record record);
  }

  private String outputLane;

  public SingleLaneProcessor() {
    setRequiresSuperInit();
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    if (getContext().getOutputLanes().size() != 1) {
      issues.add(getContext().createConfigIssue(null, null, Errors.API_00, getInfo().getInstanceName(),
                                                getContext().getOutputLanes().size()));
    } else {
      outputLane = getContext().getOutputLanes().iterator().next();
    }
    setSuperInitCalled();
    return issues;
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
