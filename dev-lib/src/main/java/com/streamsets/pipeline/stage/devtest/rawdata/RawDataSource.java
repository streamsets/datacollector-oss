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
package com.streamsets.pipeline.stage.devtest.rawdata;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.support.service.ServicesUtil;

import java.util.List;

public class RawDataSource extends BaseSource  {
  private final String rawData;
  private final String eventData;
  private final boolean stopAfterFirstBatch;

  public RawDataSource(
      String rawData,
      String eventData,
      boolean stopAfterFirstBatch
  ) {
    this.rawData = rawData;
    this.eventData = eventData;
    this.stopAfterFirstBatch = stopAfterFirstBatch;
  }

  @Override
  public List<Stage.ConfigIssue> init() {
    return super.init();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    for (Record record : ServicesUtil.parseAll(getContext(), getContext(), false, "rawData", rawData.getBytes())) {
      batchMaker.addRecord(record);
    }

    if(eventData != null && !eventData.isEmpty()) {
      for (Record record : ServicesUtil.parseAll(getContext(), getContext(), false, "eventData", eventData.getBytes())) {
        EventRecord event = getContext().createEventRecord("raw-data-event", 1, record.getHeader().getSourceId());
        event.set(record.get());
        getContext().toEvent(event);
      }
    }

    if(stopAfterFirstBatch) {
      return null;
    } else {
      return "rawData";
    }
  }

}
