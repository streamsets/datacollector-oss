/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.configurablestage;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;

import java.io.IOException;

public abstract class DSource extends DStage<Source.Context> implements Source {

  protected abstract Source createSource();

  @Override
  Stage<Source.Context> createStage() {
    return createSource();
  }

  public Source getSource() {
    return (Source)getStage();
  }

  @Override
  public final String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    return getSource().produce(lastSourceOffset, maxBatchSize, batchMaker);
  }

  @Override
  public int getParallelism() throws IOException {
    return getSource().getParallelism();
  }

}
