/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.module;

import com.streamsets.pipeline.api.v3.Batch;
import com.streamsets.pipeline.api.v3.BatchMaker;
import com.streamsets.pipeline.api.v3.Processor;
import com.streamsets.pipeline.api.v3.Processor.Context;
import com.streamsets.pipeline.api.v3.record.Record;

import java.util.Iterator;

public abstract class RecordSplitterModule extends BaseModule<Context> implements Processor {

  @Override
  public final void process(Batch batch, BatchMaker batchMaker) {
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      split(record, batchMaker);
    }
  }

  protected abstract void split(Record record, BatchMaker batchMaker);

}
