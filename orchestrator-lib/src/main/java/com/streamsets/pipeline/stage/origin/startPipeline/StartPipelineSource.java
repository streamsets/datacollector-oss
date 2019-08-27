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
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.startPipeline.PipelineIdConfig;
import com.streamsets.pipeline.lib.startPipeline.StartPipelineCommon;
import com.streamsets.pipeline.lib.startPipeline.StartPipelineConfig;
import com.streamsets.pipeline.lib.startPipeline.StartPipelineSupplier;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class StartPipelineSource extends BaseSource {

  private StartPipelineCommon startPipelineCommon;
  private StartPipelineConfig conf;

  StartPipelineSource(StartPipelineConfig conf) {
    this.startPipelineCommon = new StartPipelineCommon(conf);
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    return this.startPipelineCommon.init(issues, getContext());
  }

  @Override
  public String produce(String s, int i, BatchMaker batchMaker) throws StageException {
    List<CompletableFuture<Field>> startPipelineFutures = new ArrayList<>();
    Executor executor = Executors.newCachedThreadPool();

    for(PipelineIdConfig pipelineIdConfig: conf.pipelineIdConfigList) {
      CompletableFuture<Field> future = CompletableFuture.supplyAsync(new StartPipelineSupplier(
          this.startPipelineCommon.managerApi,
          this.startPipelineCommon.storeApi,
          conf,
          pipelineIdConfig,
          getContext()
      ), executor);
      startPipelineFutures.add(future);
    }

    try {
      LinkedHashMap<String, Field> outputField = startPipelineCommon.startPipelineInParallel(
          startPipelineFutures,
          getContext()
      );
      Record outputRecord = getContext().createRecord("startPipelineSource");
      outputRecord.set(Field.createListMap(outputField));
      batchMaker.addRecord(outputRecord);
    } catch (Exception ex) {
      getContext().reportError(ex);
    }

    return null;
  }
}
