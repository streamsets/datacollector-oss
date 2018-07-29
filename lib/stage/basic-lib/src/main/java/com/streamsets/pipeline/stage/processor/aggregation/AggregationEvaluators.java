/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http:www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.aggregation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.DataCollectorServices;
import com.streamsets.pipeline.api.ext.json.JsonMapper;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.util.JsonUtil;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.Aggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.AggregatorData;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.Aggregators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An AggregationEvaluators handles all aggregations defined by the AggregatorProcessor configuration.
 * <p/>
 * It delegates to the an AggregationEvaluator for each aggregration.
 */
public class AggregationEvaluators {
  private static final Logger LOG = LoggerFactory.getLogger(AggregationEvaluators.class);

  public static final String ALL_AGGREGATORS_EVENT = ".window.all.aggregators";
  public static final String SINGLE_AGGREGATOR_EVENT = ".window.aggregator";

  private final Processor.Context context;
  private AggregationConfigBean config;
  private final BlockingQueue<EventRecord> queue;
  private final List<AggregationEvaluator> evaluators;
  private Aggregators aggregators;
  private final ScheduledExecutorService executor;

  private long currentWindowCloseTime;

  public AggregationEvaluators(
      Processor.Context context,
      AggregationConfigBean config,
      BlockingQueue<EventRecord> queue
  ) {
    this.context = context;
    this.config = config;
    this.queue = queue;
    evaluators = new ArrayList<>();
    aggregators = new Aggregators(config.getNumberOfTimeWindows(), config.windowType);
    executor = new SafeScheduledExecutorService(1, context.getStageInfo().getInstanceName() + "_" + config.windowType);
  }

  @VisibleForTesting
  long getNowMillis() {
    return System.currentTimeMillis();
  }

  @VisibleForTesting
  long getWindowCloseTimeMilis() {
    return currentWindowCloseTime;
  }

  public void init() {
    for (AggregatorConfig aggregatorConfig : config.aggregatorConfigs) {
      if (aggregatorConfig.enabled) {
        evaluators.add(new AggregationEvaluator(
            context,
            config.windowType,
            config.getTimeWindowLabel(),
            aggregatorConfig,
            aggregators
        ));
      }
    }
    long currentTime = getNowMillis();
    currentWindowCloseTime = config.getRollingTimeWindow().getCurrentWindowCloseTimeMillis(config.getTimeZone(), currentTime);

    aggregators.start(currentWindowCloseTime);

    long initialDelay = getWindowCloseTimeMilis() - currentTime;
    executor.scheduleAtFixedRate(this::closeWindow,
        initialDelay,
        config.getRollingTimeWindow().getIntervalInMillis(),
        TimeUnit.MILLISECONDS
    );
  }

  @VisibleForTesting
  List<AggregationEvaluator> getEvaluators() {
    return evaluators;
  }

  public void destroy() {
    executor.shutdownNow();
    Map<Aggregator, AggregatorData> allAggregatorsDataMap = aggregators.stop();
    prepareEvents(allAggregatorsDataMap);
  }

  @VisibleForTesting
  void closeWindow() {
    Calendar calendar = Calendar.getInstance(config.getTimeZone());
    calendar.setTimeInMillis(getWindowCloseTimeMilis());
    Date closeWindowDate = calendar.getTime();
    LOG.debug("Closing window for '{}'", closeWindowDate);
    long newWindowCloseTime =
        config.getRollingTimeWindow().getCurrentWindowCloseTimeMillis(config.getTimeZone(), getNowMillis());
    try {
      Map<Aggregator, AggregatorData> data = aggregators.roll(newWindowCloseTime);
      prepareEvents(data);
    } catch (Exception ex) {
      LOG.error("Error closing window for '{}': {}", closeWindowDate, ex);
    }
  }

  @VisibleForTesting
  void prepareEvents(Map<Aggregator, AggregatorData> allAggregatorsDataMap) {
    if (!allAggregatorsDataMap.isEmpty() && (config.allAggregatorsEvent || config.perAggregatorEvents)) {
      List<AggregatorData> allAggregatorsData = ImmutableList.copyOf(allAggregatorsDataMap.values());
      String sdcId = context.getSdcId();
      String pipelineId = context.getPipelineId();
      long dataWindowTimeMillis = allAggregatorsData.get(0).getTime();
      if (config.allAggregatorsEvent) {
        String recordSrcId = sdcId + "::" + pipelineId + "::" + dataWindowTimeMillis;
        createEventRecord(allAggregatorsData, config.windowType + ALL_AGGREGATORS_EVENT, recordSrcId);
      }
      if (config.perAggregatorEvents) {
        for (AggregatorData aggregatorData : allAggregatorsData) {
          String recordSrcId = sdcId + "::" + pipelineId + "::" + aggregatorData.getName() + "::" + dataWindowTimeMillis;
          createEventRecord(aggregatorData, config.windowType + SINGLE_AGGREGATOR_EVENT, recordSrcId);
        }
      }
    }
  }

  public void evaluate(Record record) throws StageException {
    for (AggregationEvaluator evaluator : evaluators) {
      evaluator.evaluate(record);
    }
  }

  private void createEventRecord(Object data, String eventType, String recordSrcId) {
    EventRecord windowRollEvent = context.createEventRecord(eventType, 1, recordSrcId);
    JsonMapper json = DataCollectorServices.instance().get(JsonMapper.SERVICE_KEY);
    try {
      String jsonData = json.writeValueAsString(data);
      Field field;
      if (config.eventRecordWithTextField) {
        field = Field.create(jsonData);
      } else {
        field = JsonUtil.bytesToField((ContextExtensions) context, jsonData.getBytes());
      }
      windowRollEvent.set(field);
      queue.add(windowRollEvent);
    } catch (IOException | StageException ex) {
      context.toError(windowRollEvent, ex);
    }
  }

}
