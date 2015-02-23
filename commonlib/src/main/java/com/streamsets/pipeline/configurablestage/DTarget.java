/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.configurablestage;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;

public abstract class DTarget extends DStage<Target.Context> implements Target {

  protected abstract Target createTarget();

  @Override
  Stage<Target.Context> createStage() {
    return createTarget();
  }

  @Override
  public final void write(Batch batch) throws StageException {
    ((Target)getStage()).write(batch);
  }

}
