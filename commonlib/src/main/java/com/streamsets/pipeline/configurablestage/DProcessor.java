/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.configurablestage;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Processor;

public abstract class DProcessor extends DStage<Processor.Context> implements Processor {

  protected abstract Processor createProcessor();

  @Override
  Stage<Processor.Context> createStage() {
    return createProcessor();
  }

  @Override
  public final void process(Batch batch, BatchMaker batchMaker) throws StageException {
    ((Processor)getStage()).process(batch, batchMaker);
  }

}
