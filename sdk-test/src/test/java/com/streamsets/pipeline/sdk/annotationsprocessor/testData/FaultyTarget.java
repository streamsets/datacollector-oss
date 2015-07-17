/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor.testData;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;

//25. Target defines a on DefaultOutputStreams class
@StageDef(label = "Fault Target", version = 1, outputStreams = StageDef.VariableOutputStreams.class)
public class FaultyTarget extends BaseTarget {
  @Override
  public void write(Batch batch) throws StageException {

  }
}
