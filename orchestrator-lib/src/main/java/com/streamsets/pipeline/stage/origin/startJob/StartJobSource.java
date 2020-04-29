/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.startJob;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.CommonUtil;
import com.streamsets.pipeline.lib.startJob.StartJobCommon;
import com.streamsets.pipeline.lib.startJob.StartJobConfig;
import com.streamsets.pipeline.lib.startJob.StartJobErrors;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class StartJobSource extends BaseSource {

  private static final Logger LOG = LoggerFactory.getLogger(StartJobSource.class);
  private final StartJobCommon startJobCommon;
  private final StartJobConfig conf;
  private DefaultErrorRecordHandler errorRecordHandler;

  StartJobSource(StartJobConfig conf) {
    this.startJobCommon = new StartJobCommon(conf);
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    return this.startJobCommon.init(issues, errorRecordHandler, getContext());
  }

  @Override
  public String produce(String s, int i, BatchMaker batchMaker) throws StageException {
    Executor executor = Executors.newCachedThreadPool();
    List<CompletableFuture<Field>> startJobFutures = startJobCommon.getStartJobFutures(executor, null);
    try {
      LinkedHashMap<String, Field> outputField = startJobCommon.startJobInParallel(startJobFutures);
      Record outputRecord = CommonUtil.createOrchestratorTaskRecord(
          null,
          getContext(),
          conf.taskName,
          outputField
      );
      batchMaker.addRecord(outputRecord);
    } catch (Exception ex) {
      LOG.error(ex.toString(), ex);
      errorRecordHandler.onError(StartJobErrors.START_JOB_08, ex.toString(), ex);
    }
    return null;
  }
}
