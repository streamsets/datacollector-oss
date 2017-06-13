/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

  public void handle(String sourceEntity, String sourceOffset, List<Record> badRecords) throws StageException {
    // Shortcut to avoid synchronization if there are no error records
    if(badRecords.isEmpty()) {
      return;
    }

    synchronized (errorStage) {
      errorStage.execute(
        sourceOffset,     // Source offset for this batch
        -1,     // BatchSize is not used for target
        new BatchImpl("errorStage", sourceEntity, sourceOffset, badRecords),
        null,  // BatchMaker doesn't make sense for target
        null,    // Error stage can't generate error records
        null    // And also can't generate events
      );
    }
  }

  public void destroy() {
    errorStage.getStage().destroy();
  }

}
