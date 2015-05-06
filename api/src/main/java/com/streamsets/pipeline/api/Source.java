/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

import java.util.List;

public interface Source extends Stage<Source.Context> {

  public interface Context extends Stage.Context {

    public List<String> getOutputLanes();

  }

  // returns offset NULL if done
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException;

  public int getParallelism() throws StageException;
}
