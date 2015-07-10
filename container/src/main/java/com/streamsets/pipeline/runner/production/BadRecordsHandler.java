/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.runner.BatchImpl;
import com.streamsets.pipeline.runner.StageRuntime;
import com.streamsets.pipeline.validation.Issue;

import java.util.List;

public class BadRecordsHandler {
  private final StageRuntime errorStage;

  public BadRecordsHandler(StageRuntime errorStage) {
    this.errorStage = errorStage;
  }

  public List<Issue> validate() throws StageException {
    return  errorStage.validateConfigs();
  }

  public void init() throws StageException {
    errorStage.init();
  }

  public void handle(String sourceOffset, List<Record> badRecords) throws StageException {
    ((Target)errorStage.getStage()).write(new BatchImpl("errorStage", sourceOffset, badRecords));
  }

  public void destroy() {
    errorStage.destroy();
  }

}
