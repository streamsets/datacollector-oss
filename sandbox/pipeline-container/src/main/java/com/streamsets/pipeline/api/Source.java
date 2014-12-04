/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

public interface Source extends Module<Source.Context> {

  public interface Context extends Module.Context {

    public Record createRecord(String sourceInfo);

    public Record createRecord(String sourceInfo, byte[] raw, String rawMime);

  }

  public String produce(String lastBatchId, BatchMaker batchMaker); // returns batchId, NULL if done

}
