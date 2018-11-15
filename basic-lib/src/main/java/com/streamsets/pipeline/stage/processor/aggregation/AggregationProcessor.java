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
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class AggregationProcessor extends SingleLaneProcessor {
  private final static String EVALUATORS = "evaluators";
  private final static String EVENT_RECORDS_QUEUE = "event.records.queue";

  private final AggregationConfigBean config;
  private AggregationEvaluators evaluators;
  private BlockingQueue<EventRecord> eventRecordsQueue;

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
        Map<String, Object> stageRunnerSharedMap = getContext().getStageRunnerSharedMap();
        synchronized (stageRunnerSharedMap) {
          evaluators = (AggregationEvaluators) stageRunnerSharedMap.get(EVALUATORS);
          eventRecordsQueue = (BlockingQueue<EventRecord>)stageRunnerSharedMap.get(EVENT_RECORDS_QUEUE);
          if (evaluators == null) {
            eventRecordsQueue = createEventRecordsQueue();
            stageRunnerSharedMap.put(EVENT_RECORDS_QUEUE, eventRecordsQueue);
            evaluators = new AggregationEvaluators(getContext(), config, eventRecordsQueue);
            evaluators.init();
            stageRunnerSharedMap.put(EVALUATORS, evaluators);
          }
        }
      }
    }
    return configIssues;
  }

  @Override
  public void process(
      Batch batch, SingleLaneBatchMaker singleLaneBatchMaker
  ) throws StageException {
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      evaluators.evaluate(record);
      singleLaneBatchMaker.addRecord(record);
    }
    publishEventRecordsIfAny();
  }

  @Override
  public void destroy() {
    synchronized (AggregationProcessor.class) {
      if (evaluators != null && getContext().getStageRunnerSharedMap().get(EVALUATORS) != null) {
        evaluators.destroy();
        evaluators = null;
        publishEventRecordsIfAny();
        getContext().getStageRunnerSharedMap().remove(EVALUATORS);
      }
    }
    super.destroy();
  }

  @VisibleForTesting
  AggregationEvaluators getEvaluators() {
    return evaluators;
  }

  private synchronized void publishEventRecordsIfAny() {
    if (eventRecordsQueue.size() > 0) {
      List<EventRecord> eventList = new ArrayList<>();
      eventRecordsQueue.drainTo(eventList);
      for (EventRecord r : eventList) {
        getContext().toEvent(r);
      }
    }
  }

  @VisibleForTesting
  BlockingQueue<EventRecord> createEventRecordsQueue() {
    /*
     * A separate thread monitors the time window and creates event records when window is rolled over.
     * Therefore those event records cannot be sent to event sink from that thread (as the stage runtime may have been
     * destroyed by then).
     *
     * hence this blocking queue ensures that records are sent to event sink only from within the process method.
     */

    int size = 0;
    if (config.perAggregatorEvents) {
      size += config.aggregatorConfigs.size();
    }
    if (config.allAggregatorsEvent) {
      size += 1;
    }
    /*
     * It is possible that the window size is small and the batch processing time is larger. In that case the more event
     * records can be added to the event queue before it is drained. Therefore multiple size by 3 to account for
     * 3 event rolls before the event is drained
     */
    size = size > 0 ? size*3 : 1;
    return new ArrayBlockingQueue<>(size);
  }

}
