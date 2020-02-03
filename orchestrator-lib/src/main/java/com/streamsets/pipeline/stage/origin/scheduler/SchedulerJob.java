/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.scheduler;

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.LinkedHashMap;

public class SchedulerJob implements Job {

  private static final Logger LOG = LoggerFactory.getLogger(SchedulerJob.class);

  @Override
  public void execute(JobExecutionContext jobExecutionContext) {
    try {
      SchedulerContext schedulerContext = jobExecutionContext.getScheduler().getContext();
      PushSource.Context pushSourceContext = (PushSource.Context) schedulerContext.get(
          SchedulerPushSource.PUSH_SOURCE_CONTEXT
      );
      if (pushSourceContext != null) {
        BatchContext batchContext = pushSourceContext.startBatch();
        Record record = pushSourceContext.createRecord("cronRecord");
        LinkedHashMap<String, Field> linkedHashMap = new LinkedHashMap<>();
        linkedHashMap.put("timestamp", Field.createDatetime(new Date()));
        record.set(Field.createListMap(linkedHashMap));
        batchContext.getBatchMaker().addRecord(record);
        pushSourceContext.processBatch(batchContext);
      }
    } catch (SchedulerException ex) {
      LOG.error(ex.getMessage(), ex);
    }
  }
}
