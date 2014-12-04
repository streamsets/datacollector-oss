/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

public interface Processor extends Module<Processor.Context> {

  public interface Context extends Module.Context {

    public Record createRecord(String sourceInfo);

    public Record cloneRecord(Record record);

  }

  public void process(Batch batch, BatchMaker batchMaker);

}
