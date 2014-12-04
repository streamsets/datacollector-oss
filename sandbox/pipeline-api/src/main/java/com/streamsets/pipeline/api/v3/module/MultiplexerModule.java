/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.module;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.v3.Batch;
import com.streamsets.pipeline.api.v3.BatchMaker;
import com.streamsets.pipeline.api.v3.Processor;
import com.streamsets.pipeline.api.v3.Processor.Context;
import com.streamsets.pipeline.api.v3.record.Record;

import java.util.Iterator;

public abstract class MultiplexerModule extends BaseModule<Context> implements Processor {

  @Override
  public final void process(Batch batch, BatchMaker batchMaker) {
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      String[] tags = multiplex(record);
      if (tags != null && tags.length > 0) {
        for (String tag : tags){
          Preconditions.checkNotNull(tag, "tags cannot be null");
          batchMaker.addRecord(getContext().cloneRecord(record));
        }
      }
    }
  }

  protected abstract String[] multiplex(Record record);

}
