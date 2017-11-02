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
package com.streamsets.pipeline.stage.processor.aggregation;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class AggregationProcessor extends SingleLaneRecordProcessor {
  private final static String EVALUATORS = "evaluators";
  public static final int EVENT_RECORD_QUEUE_CAPACITY = 100;

  private final AggregationConfigBean config;
  private AggregationEvaluators evaluators;

  /*
   * A separate thread monitors the time window and creates event records when window is rolled over.
   * Therefore those event records cannot be sent to event sink from that thread (as the stage runtime may have been
   * destroyed by then).
   *
   * hence this blocking queue ensures that records are sent to event sink only from within the process method.
   */
  private final BlockingQueue<EventRecord> eventRecordsQueue = new ArrayBlockingQueue<>(EVENT_RECORD_QUEUE_CAPACITY);


  public AggregationProcessor(AggregationConfigBean config) {
    this.config = config;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> configIssues = super.init();
    if (configIssues.isEmpty()) {
      configIssues.addAll(config.init(getContext()));
      if (configIssues.isEmpty()) {
        // we are using the stage context to make sure we have one per pipeline even if multithreaded origin
        synchronized (getClass()) {
          evaluators = (AggregationEvaluators) getContext().getStageRunnerSharedMap().get(EVALUATORS);
          if (evaluators == null) {
            evaluators = new AggregationEvaluators(getContext(), config, eventRecordsQueue);
            evaluators.init();
          }
        }
      }
    }
    return configIssues;
  }

  @Override
  public void destroy() {
    synchronized (getClass()) {
      if (evaluators != null) {
        evaluators.destroy();
        evaluators = null;
        publishEventRecordsIfAny();
      }
    }
    super.destroy();
  }

  @VisibleForTesting
  AggregationEvaluators getEvaluators() {
    return evaluators;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    publishEventRecordsIfAny();
    evaluators.evaluate(record);
    batchMaker.addRecord(record);
  }

  private void publishEventRecordsIfAny() {
    if (eventRecordsQueue.size() > 0) {
      List<EventRecord> eventList = new ArrayList<>(EVENT_RECORD_QUEUE_CAPACITY);
      eventRecordsQueue.drainTo(eventList);
      for (EventRecord r : eventList) {
        getContext().toEvent(r);
      }
    }
  }

}
