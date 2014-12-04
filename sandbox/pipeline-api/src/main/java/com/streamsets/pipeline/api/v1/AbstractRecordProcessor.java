/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v1;

public abstract class AbstractRecordProcessor extends AbstractProcessor {

  @Override
  public void process(Batch batch, BatchMaker batchMaker) {
    for (Record record : batch.getAllRecords()) {
      batchMaker.addRecord(process(record));
    }
  }

  protected abstract Record process(Record record);

}
