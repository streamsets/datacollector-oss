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
package com.streamsets.pipeline.stage.processor.startJob;

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
import com.streamsets.pipeline.lib.startJob.JobIdConfig;
import com.streamsets.pipeline.lib.startJob.StartJobCommon;
import com.streamsets.pipeline.lib.startJob.StartJobConfig;
import com.streamsets.pipeline.lib.startJob.StartJobSupplier;
import com.streamsets.pipeline.lib.startJob.StartJobTemplateSupplier;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@SuppressWarnings("unchecked")
public class StartJobProcessor extends SingleLaneProcessor {

  private StartJobCommon startJobCommon;
  private StartJobConfig conf;

  private ELVars jobIdConfigVars;
  private ELEval jobIdEval;
  private ELEval runtimeParametersEval;

  StartJobProcessor(StartJobConfig conf) {
    this.conf = conf;
    this.startJobCommon = new StartJobCommon(conf);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    jobIdConfigVars = getContext().createELVars();
    jobIdEval = getContext().createELEval("jobId");
    runtimeParametersEval = getContext().createELEval("runtimeParameters");
    return this.startJobCommon.init(issues);
  }

  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    List<CompletableFuture<Field>> startJobFutures = new ArrayList<>();
    Executor executor = Executors.newCachedThreadPool();
    Iterator<Record> it = batch.getRecords();
    Record firstRecord = null;
    while (it.hasNext()) {
      Record record = it.next();
      if (firstRecord == null) {
        firstRecord = record;
      }
      try {
        if (conf.jobTemplate) {
          RecordEL.setRecordInContext(jobIdConfigVars, record);
          TimeNowEL.setTimeNowInContext(jobIdConfigVars, new Date());
          String templateJobId = jobIdEval.eval(
              jobIdConfigVars,
              conf.templateJobId,
              String.class
          );
          String runtimeParametersList = runtimeParametersEval.eval(
              jobIdConfigVars,
              conf.runtimeParametersList,
              String.class
          );
          CompletableFuture<Field> future = CompletableFuture.supplyAsync(new StartJobTemplateSupplier(
              conf,
              templateJobId,
              runtimeParametersList,
              getContext()
          ), executor);
          startJobFutures.add(future);
        } else {
          for(JobIdConfig jobIdConfig: conf.jobIdConfigList) {
            JobIdConfig resolvedJobIdConfig = new JobIdConfig();
            RecordEL.setRecordInContext(jobIdConfigVars, record);
            TimeNowEL.setTimeNowInContext(jobIdConfigVars, new Date());
            resolvedJobIdConfig.jobId = jobIdEval.eval(
                jobIdConfigVars,
                jobIdConfig.jobId,
                String.class
            );
            resolvedJobIdConfig.runtimeParameters = runtimeParametersEval.eval(
                jobIdConfigVars,
                jobIdConfig.runtimeParameters,
                String.class
            );
            CompletableFuture<Field> future = CompletableFuture.supplyAsync(new StartJobSupplier(
                conf,
                resolvedJobIdConfig,
                getContext()
            ), executor);
            startJobFutures.add(future);
          }
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

    if (startJobFutures.isEmpty()) {
      return;
    }

    try {
      LinkedHashMap<String, Field> outputField = startJobCommon.startJobInParallel(
          startJobFutures,
          getContext()
      );

      if (firstRecord == null) {
        firstRecord = getContext().createRecord("startJobProcessor");
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
