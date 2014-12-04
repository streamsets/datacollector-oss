/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3;

import com.streamsets.pipeline.api.v3.record.Record;

// it must be @Module annotated
public interface Processor extends Module<Processor.Context> {

  public interface Context extends Module.Context {

    public Record createRecord(Record parentRecord);

    public Record cloneRecord(Record record);

  }

  public void process(Batch batch, BatchMaker batchMaker);

}
