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
package com.streamsets.pipeline.sdk;

import com.streamsets.datacollector.runner.StageContext;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;

import java.util.Collection;
import java.util.List;

/**
 * SDK implementation of the BatchContext interface
 */
public class BatchContextSdkImpl implements BatchContext {

  private BatchMaker batchMaker;
  private StageContext context;

  public BatchContextSdkImpl(BatchMaker batchMaker, StageContext context) {
    this.batchMaker = batchMaker;
    this.context = context;
  }

  @Override
  public BatchMaker getBatchMaker() {
    return batchMaker;
  }

  @Override
  public void toError(Record record, Exception exception) {
    context.toError(record, exception);
  }

  @Override
  public void toError(Record record, String errorMessage) {
    context.toError(record, errorMessage);
  }

  @Override
  public void toError(Record record, ErrorCode errorCode, Object... args) {
    context.toError(record, errorCode, args);
  }

  @Override
  public void toEvent(EventRecord record) {
    context.toEvent(record);
  }

  @Override
  public void complete(Record record) {
    context.complete(record);
  }

  @Override
  public void complete(Collection<Record> records) {
    context.complete(records);
  }

  @Override
  public List<Record> getSourceResponseRecords() {
    return context.getSourceResponseSink().getResponseRecords();
  }

}
