/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.stagelibrary;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;

import java.io.IOException;
import java.util.List;

@StageDef(version = 1, label = "")
public class ForTestSource implements Source {

  @ConfigDef(type = ConfigDef.Type.STRING, label = "", required = true)
  public String foo;

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    return null;
  }

  @Override
  public int getParallelism() throws IOException {
    return 0;
  }

  @Override
  public List<ConfigIssue> init(Info info, Context context) {
    return null;
  }

  @Override
  public void destroy() {

  }

}
