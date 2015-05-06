/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;

public abstract class BaseSource extends BaseStage<Source.Context> implements Source {

  @Override
  public int getParallelism() throws StageException {
    return 1;
  }
}
