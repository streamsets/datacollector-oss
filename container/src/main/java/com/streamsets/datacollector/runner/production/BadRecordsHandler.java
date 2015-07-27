/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner.production;

import com.streamsets.datacollector.runner.BatchImpl;
import com.streamsets.datacollector.runner.StagePipe;
import com.streamsets.datacollector.runner.StageRuntime;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;

import java.util.List;

public class BadRecordsHandler {
  private final StageRuntime errorStage;

  public BadRecordsHandler(StageRuntime errorStage) {
    this.errorStage = errorStage;
  }

  public String getInstanceName() {
    return errorStage.getInfo().getInstanceName();
  }

  public List<Issue> init(StagePipe.Context context) {
    return  errorStage.init();
  }

  public void handle(String sourceOffset, List<Record> badRecords) throws StageException {
    ((Target)errorStage.getStage()).write(new BatchImpl("errorStage", sourceOffset, badRecords));
  }

  public void destroy() {
    errorStage.destroy();
  }

}
