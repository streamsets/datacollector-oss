package com.streamsets.pipeline.sdk.test;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

/**
 * Created by harikiran on 10/22/14.
 */
@StageDef(name = "TwitterSource", description = "Produces twitter feeds", label = "twitter_source"
, version = "1.0")
public class SourceWithOnlyStageDef extends BaseSource{

  public SourceWithOnlyStageDef() {
  }

  @Override
  public String produce(String lastBatchId, BatchMaker batchMaker) throws StageException {
    return null;
  }
}