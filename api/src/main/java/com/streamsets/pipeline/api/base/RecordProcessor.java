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
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Iterator;

public abstract class RecordProcessor extends BaseProcessor {

  @Override
  public void process(Batch batch, BatchMaker batchMaker) throws StageException {
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      try {
        process(record, batchMaker);
      } catch (OnRecordErrorException ex) {
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            getContext().toError(record, ex);
            break;
          case STOP_PIPELINE:
            throw ex;
          default:
            throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
                                                         getContext().getOnErrorRecord(), ex));
        }
      }
    }
  }

  protected abstract void process(Record record, BatchMaker batchMaker) throws StageException, OnRecordErrorException;

}
