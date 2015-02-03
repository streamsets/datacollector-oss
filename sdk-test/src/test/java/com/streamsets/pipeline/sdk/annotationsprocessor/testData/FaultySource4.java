/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor.testData;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

//29. VariableOutputStreams and non existing outputStreamsDrivenByConfig
@StageDef(description = "Produces twitter feeds", label = "twitter_source"
  , version = "1.0", outputStreams = StageDef.VariableOutputStreams.class, outputStreamsDrivenByConfig = "xyz")
public class FaultySource4 extends BaseSource {
  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    return null;
  }
}
