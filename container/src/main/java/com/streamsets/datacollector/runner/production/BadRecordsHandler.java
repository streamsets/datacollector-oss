/*
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

import com.streamsets.datacollector.config.ErrorRecordPolicy;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.BatchImpl;
import com.streamsets.datacollector.runner.ErrorSink;
import com.streamsets.datacollector.runner.StagePipe;
import com.streamsets.datacollector.runner.StageRuntime;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BadRecordsHandler {
  private final ErrorRecordPolicy errorRecordPolicy;
  private final RuntimeInfo runtimeInfo;
  private final String pipelineName;
  private final StageRuntime errorStage;

  public BadRecordsHandler(
    ErrorRecordPolicy errorRecordPolicy,
    RuntimeInfo runtimeInfo,
    StageRuntime errorStage,
    String pipelineName
  ) {
    this.errorRecordPolicy = errorRecordPolicy;
    this.runtimeInfo = runtimeInfo;
    this.errorStage = errorStage;
    this.pipelineName = pipelineName;
  }

  public String getInstanceName() {
    return errorStage.getInfo().getInstanceName();
  }

  public List<Issue> init(StagePipe.Context context) {
    return  errorStage.init();
  }

  public void handle(String sourceEntity, String sourceOffset, ErrorSink errorSink) throws StageException {
    // Get error records from the error sink
    List<Record> badRecords = getBadRecords(errorSink);

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

  /**
   * Generate list of error records from the error sink. What precise records will be returned depends on the error
   * record policy configuration.
   *
   * @param errorSink Error sink with the error records
   * @return List with records that should be sent to error stream
   */
  private List<Record> getBadRecords(ErrorSink errorSink) {
    List<Record> badRecords = new ArrayList<>();

    for (Map.Entry<String, List<Record>> entry : errorSink.getErrorRecords().entrySet()) {
      for (Record record : entry.getValue()) {
        RecordImpl errorRecord;

        switch (errorRecordPolicy) {
          case ORIGINAL_RECORD:
            errorRecord = (RecordImpl) ((RecordImpl)record).getHeader().getSourceRecord();
            errorRecord.getHeader().copyErrorFrom(record);
            break;
          case STAGE_RECORD:
            errorRecord = (RecordImpl) record;
            break;
          default:
           throw new IllegalArgumentException("Uknown error record policy: " + errorRecordPolicy);
        }

        errorRecord.getHeader().setErrorContext(runtimeInfo.getId(), pipelineName);
        badRecords.add(errorRecord);
      }
    }
    return badRecords;
  }

}
