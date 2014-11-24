/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.runner;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.metrics.MetricsConfigurator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StagePipe extends Pipe {
  private Timer processingTimer;
  private Counter inputRecordsCounter;
  private Counter outputRecordsCounter;
  private Counter errorRecordsCounter;
  private Meter inputRecordsMeter;
  private Meter outputRecordsMeter;
  private Meter errorRecordsMeter;
  private Map<String, Counter> outputRecordsPerLaneCounter;
  private Map<String, Meter> outputRecordsPerLaneMeter;

  public StagePipe(StageRuntime stage, List<String> inputLanes, List<String> outputLanes) {
    super(stage, inputLanes, outputLanes);
  }

  @Override
  public void init() throws StageException {
    getStage().init();
    MetricRegistry metrics = getStage().getContext().getMetrics();
    String metricsKey = "stage." + getStage().getConfiguration().getInstanceName();
    processingTimer = MetricsConfigurator.createTimer(metrics, metricsKey + ".batchProcessing");
    inputRecordsCounter = MetricsConfigurator.createCounter(metrics, metricsKey + ".inputRecords");
    outputRecordsCounter = MetricsConfigurator.createCounter(metrics, metricsKey + ".outputRecords");
    errorRecordsCounter = MetricsConfigurator.createCounter(metrics, metricsKey + ".errorRecords");
    inputRecordsMeter = MetricsConfigurator.createMeter(metrics, metricsKey + ".inputRecords");
    outputRecordsMeter = MetricsConfigurator.createMeter(metrics, metricsKey + ".outputRecords");
    errorRecordsMeter = MetricsConfigurator.createMeter(metrics, metricsKey + ".errorRecords");
    if (getStage().getConfiguration().getOutputLanes().size() > 1) {
      outputRecordsPerLaneCounter = new HashMap<String, Counter>();
      outputRecordsPerLaneMeter = new HashMap<String, Meter>();
      for (String lane : getStage().getConfiguration().getOutputLanes()) {
        outputRecordsPerLaneCounter.put(lane, MetricsConfigurator.createCounter(
            metrics, metricsKey + ":" + lane + ".outputRecords"));
        outputRecordsPerLaneMeter.put(lane, MetricsConfigurator.createMeter(
            metrics, metricsKey + ":" + lane + ".outputRecords"));
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void process(PipeBatch pipeBatch) throws StageException, PipelineRuntimeException {
    BatchMakerImpl batchMaker = pipeBatch.startStage(this);
    BatchImpl batchImpl = pipeBatch.getBatch(this);
    ErrorRecordSink errorRecordSink = pipeBatch.getErrorRecordSink();
    inputRecordsCounter.inc(batchImpl.getSize());
    inputRecordsMeter.mark(batchImpl.getSize());

    RequiredFieldsErrorPredicateSink predicateSink = new RequiredFieldsErrorPredicateSink(
        getStage().getInfo().getInstanceName(), getStage().getRequiredFields(), errorRecordSink);
    Batch batch = new FilterRecordBatch(batchImpl, predicateSink, predicateSink);

    long start = System.currentTimeMillis();
    String newOffset = getStage().execute(pipeBatch.getPreviousOffset(), pipeBatch.getBatchSize(), batch, batchMaker,
                                          errorRecordSink);
    if (getStage().getDefinition().getType() == StageType.SOURCE) {
      pipeBatch.setNewOffset(newOffset);
    }
    processingTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
    outputRecordsCounter.inc(batchMaker.getSize());
    outputRecordsMeter.mark(batchMaker.getSize());
    int stageErrorSinkCount = errorRecordSink.getErrorRecords(getStage().getInfo().getInstanceName()).getErrorRecords().
        size();
    errorRecordsCounter.inc(predicateSink.size() + stageErrorSinkCount);
    errorRecordsMeter.mark(predicateSink.size() + stageErrorSinkCount);
    if (getStage().getConfiguration().getOutputLanes().size() > 1) {
      for (String lane : getStage().getConfiguration().getOutputLanes()) {
        outputRecordsPerLaneCounter.get(lane).inc(batchMaker.getSize(lane));
        outputRecordsPerLaneMeter.get(lane).mark(batchMaker.getSize(lane));
      }
    }
    pipeBatch.completeStage(batchMaker);
  }

  @Override
  public void destroy() {
    getStage().destroy();
  }


}
