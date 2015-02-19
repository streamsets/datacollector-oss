/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.devtest;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;

import java.util.Iterator;

@GenerateResourceBundle
@StageDef(version = "1.0.0", label = "Dev Record Creator",
          description = "It creates 2 records from each original record")
public class RecordCreatorProcessor extends SingleLaneProcessor {

  @Override
  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws
      StageException {
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      Record record1 = getContext().cloneRecord(record);
      Record record2 = getContext().cloneRecord(record);
      record1.getHeader().setAttribute("expanded", "1");
      record2.getHeader().setAttribute("expanded", "2");
      batchMaker.addRecord(record1);
      batchMaker.addRecord(record2);
    }
  }

}
