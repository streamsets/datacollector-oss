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
package com.streamsets.datacollector.runner;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ToErrorContext;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.Errors;

import java.util.Iterator;
import java.util.Optional;
import java.util.List;
import java.util.LinkedList;

/**
 * Filter record entering stage.
 *
 * This filter will work properly only for Processor, Destination and Executors. It does not make sense and will not
 * work properly for sources (especially for PushSource).
 */
public class FilterRecordBatch implements Batch {
  private final Batch batch;
  private final Predicate[] predicates;
  private final DefaultErrorRecordHandler errorHandler;
  private List<Record> filteredRecords;

  public interface Predicate {

    public boolean evaluate(Record record);

    public ErrorMessage getRejectedMessage();

  }

  public interface Sink {

    public void add(Record record, ErrorMessage message);

  }

  public FilterRecordBatch(Batch batch, Predicate[] predicates, Stage.Context context) {
    this.batch = batch;
    this.predicates = predicates;
    // Some stages can use '@HideConfigs(onErrorRecord = true)' to hide the on-error-record behavior. In such case
    // we default to TO_ERROR to preserve behavior with older SDC versions.
    this.errorHandler = new DefaultErrorRecordHandler(
      Optional.ofNullable(context.getOnErrorRecord()).orElse(OnRecordError.TO_ERROR),
      context,
      (ToErrorContext) context
    );
  }

  @Override
  public String getSourceEntity() {
    return batch.getSourceEntity();
  }

  @Override
  public String getSourceOffset() {
    return batch.getSourceOffset();
  }

  @Override
  public Iterator<Record> getRecords() {
    if (filteredRecords == null) {
      filteredRecords = new LinkedList<>();
      Iterator<Record> it = batch.getRecords();
      while (it.hasNext()) {
        boolean passed = true;
        ErrorMessage rejectedMessage = null;
        Record record = it.next();
        for (Predicate predicate : predicates) {
          passed = predicate.evaluate(record);
          if (!passed) {
            rejectedMessage = predicate.getRejectedMessage();
            errorHandler.onError(new OnRecordErrorException(record, Errors.COMMON_0001, rejectedMessage.toString()));
            break;
          }
        }
        if (passed) {
          filteredRecords.add(record);
        }
      }
    }

    return filteredRecords.iterator();
  }
}
