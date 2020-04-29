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
package com.streamsets.pipeline.stage.origin.startPipeline;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.CommonUtil;
import com.streamsets.pipeline.lib.startPipeline.PipelineIdConfig;
import com.streamsets.pipeline.lib.startPipeline.StartPipelineCommon;
import com.streamsets.pipeline.lib.startPipeline.StartPipelineConfig;
import com.streamsets.pipeline.lib.startPipeline.StartPipelineErrors;
import com.streamsets.pipeline.lib.startPipeline.StartPipelineSupplier;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class StartPipelineSource extends BaseSource {

  private static final Logger LOG = LoggerFactory.getLogger(StartPipelineSource.class);
  private final StartPipelineCommon startPipelineCommon;
  private final StartPipelineConfig conf;
  private DefaultErrorRecordHandler errorRecordHandler;

  StartPipelineSource(StartPipelineConfig conf) {
    this.startPipelineCommon = new StartPipelineCommon(conf);
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<Stage.ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    return this.startPipelineCommon.init(issues, errorRecordHandler, getContext());
  }

  @Override
  public String produce(String s, int i, BatchMaker batchMaker) throws StageException {
    List<CompletableFuture<Field>> startPipelineFutures = new ArrayList<>();
    Executor executor = Executors.newCachedThreadPool();

    for (PipelineIdConfig pipelineIdConfig: conf.pipelineIdConfigList) {
      StartPipelineSupplier startPipelineSupplier = startPipelineCommon.getStartPipelineSupplier(
          pipelineIdConfig,
          null
      );
      CompletableFuture<Field> future = CompletableFuture.supplyAsync(startPipelineSupplier, executor);
      startPipelineFutures.add(future);
    }

    try {
      LinkedHashMap<String, Field> outputField = startPipelineCommon.startPipelineInParallel(startPipelineFutures);
      Record outputRecord = CommonUtil.createOrchestratorTaskRecord(
          null,
          getContext(),
          conf.taskName,
          outputField
      );
      batchMaker.addRecord(outputRecord);
    } catch (Exception ex) {
      LOG.error(ex.toString(), ex);
      errorRecordHandler.onError(StartPipelineErrors.START_PIPELINE_04, ex.toString(), ex);
    }

    return null;
  }
}
