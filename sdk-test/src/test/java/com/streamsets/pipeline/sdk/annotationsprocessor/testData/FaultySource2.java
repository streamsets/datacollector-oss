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

//28. VariableOutputStreams but no outputStreamsDrivenByConfig
@StageDef(description = "Produces twitter feeds", label = "twitter_source"
  , version = 1, outputStreams = StageDef.VariableOutputStreams.class)
public class FaultySource2 extends BaseSource {


  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    return null;
  }
}
