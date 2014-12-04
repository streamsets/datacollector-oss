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

import java.util.Iterator;

public abstract class RecordProcessor extends BaseProcessor {

  @Override
  public final void process(Batch batch, BatchMaker batchMaker) throws StageException {
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      process(it.next(), batchMaker);
    }
  }

  protected abstract void process(Record record, BatchMaker batchMaker) throws StageException;

}
