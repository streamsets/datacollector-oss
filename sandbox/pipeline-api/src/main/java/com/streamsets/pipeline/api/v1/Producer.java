/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v1;

// it must be @Module annotated
public interface Producer extends Module {

  public String produce(String lastBatchId, BatchMaker batchMaker); // returns batchId, NULL if done

}
