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
package com.streamsets.pipeline.stage.processor.startPipeline;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.startPipeline.PipelineIdConfig;
import com.streamsets.pipeline.lib.startPipeline.StartPipelineCommon;
import com.streamsets.pipeline.lib.startPipeline.StartPipelineConfig;
import com.streamsets.pipeline.lib.startPipeline.StartPipelineSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class StartPipelineProcessor extends SingleLaneProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(StartPipelineProcessor.class);
  private StartPipelineCommon startPipelineCommon;
  private StartPipelineConfig conf;

  private ELVars pipelineIdConfigVars;
  private ELEval pipelineIdEval;
  private ELEval runtimeParametersEval;

  StartPipelineProcessor(StartPipelineConfig conf) {
    this.startPipelineCommon = new StartPipelineCommon(conf);
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    pipelineIdConfigVars = getContext().createELVars();
    pipelineIdEval = getContext().createELEval("pipelineId");
    runtimeParametersEval = getContext().createELEval("runtimeParameters");
    return this.startPipelineCommon.init(issues, getContext());
  }

  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    List<CompletableFuture<Field>> startPipelineFutures = new ArrayList<>();
    Executor executor = Executors.newCachedThreadPool();
    Iterator<Record> it = batch.getRecords();
    Record firstRecord = null;
    while (it.hasNext()) {
      Record record = it.next();
      if (firstRecord == null) {
        firstRecord = record;
      }
      try {
        for(PipelineIdConfig pipelineIdConfig: conf.pipelineIdConfigList) {
          PipelineIdConfig resolvedPipelineIdConfig = new PipelineIdConfig();
          RecordEL.setRecordInContext(pipelineIdConfigVars, record);
          TimeNowEL.setTimeNowInContext(pipelineIdConfigVars, new Date());
          resolvedPipelineIdConfig.pipelineId = pipelineIdEval.eval(
              pipelineIdConfigVars,
              pipelineIdConfig.pipelineId,
              String.class
          );
          resolvedPipelineIdConfig.runtimeParameters = runtimeParametersEval.eval(
              pipelineIdConfigVars,
              pipelineIdConfig.runtimeParameters,
              String.class
          );

          CompletableFuture<Field> future = CompletableFuture.supplyAsync(new StartPipelineSupplier(
              this.startPipelineCommon.managerApi,
              this.startPipelineCommon.storeApi,
              conf,
              resolvedPipelineIdConfig,
              getContext()
          ), executor);
          startPipelineFutures.add(future);
        }

      } catch (OnRecordErrorException ex) {
        switch (this.getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            this.getContext().toError(record, ex);
            break;
          case STOP_PIPELINE:
            throw ex;
          default:
            throw new IllegalStateException(Utils.format(
                "It should never happen. OnError '{}'",
                this.getContext().getOnErrorRecord(),
                ex
            ));
        }
      }
    }

    if (startPipelineFutures.isEmpty()) {
      return;
    }

    try {
      LinkedHashMap<String, Field> outputField = startPipelineCommon.startPipelineInParallel(
          startPipelineFutures,
          getContext()
      );

      if (firstRecord == null) {
        firstRecord = getContext().createRecord("startPipelineProcessor");
        firstRecord.set(Field.createListMap(outputField));
      } else {
        Field rootField = firstRecord.get();
        if (rootField.getType() == Field.Type.LIST_MAP) {
          // If the root field merge results with existing record
          LinkedHashMap<String, Field> currentField = rootField.getValueAsListMap();
          currentField.putAll(outputField);
        } else {
          firstRecord.set(Field.createListMap(outputField));
        }
      }
      batchMaker.addRecord(firstRecord);
    } catch (Exception ex) {
      getContext().reportError(ex);
    }
  }
}
