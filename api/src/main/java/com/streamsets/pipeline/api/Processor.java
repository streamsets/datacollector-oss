/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

import java.util.List;

public interface Processor extends Stage<Processor.Context> {

  public interface Context extends Stage.Context {

    public List<String> getOutputLanes();

    public Record createRecord(Record originatorRecord);

    public Record createRecord(Record originatorRecord, byte[] raw, String rawMime);

    public Record cloneRecord(Record record);

  }

  public void process(Batch batch, BatchMaker batchMaker) throws StageException;

}
