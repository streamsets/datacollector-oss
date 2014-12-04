/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3;

import com.streamsets.pipeline.api.v3.record.Record;

// it must be @Module annotated
public interface Source extends Module<Source.Context> {

  public interface Context extends Module.Context {

    public Record createRecord(String recordLocation);

  }

  public String produce(String lastBatchId, BatchMaker batchMaker); // returns batchId, NULL if done

}
