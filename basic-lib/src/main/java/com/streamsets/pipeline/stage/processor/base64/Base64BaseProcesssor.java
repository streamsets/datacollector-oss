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
package com.streamsets.pipeline.stage.processor.base64;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.lib.util.CommonError;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;

import java.util.Iterator;
import java.util.List;

abstract class Base64BaseProcesssor extends SingleLaneRecordProcessor {
  private final String originFieldPath;
  private final String resultFieldPath;
  private ErrorRecordHandler errorRecordHandler;

  protected Base64BaseProcesssor(String originFieldPath, String resultFieldPath) {
    this.originFieldPath = originFieldPath;
    this.resultFieldPath = resultFieldPath;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    return issues;
  }

  @Override
  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    Iterator<Record> records = batch.getRecords();
    while (records.hasNext()) {
      process(records.next(), batchMaker);
    }
  }

  @Override
  public void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Field original = record.get(originFieldPath);
    try {
      if (original != null) {
        if (original.getType() != Field.Type.BYTE_ARRAY) {
          throw new OnRecordErrorException(
              CommonError.CMN_0100, original.getType().toString(), original.getValue().toString(), record.toString());
        }
        record.set(resultFieldPath, processField(record, original.getValueAsByteArray()));
      }
      batchMaker.addRecord(record);
    } catch (OnRecordErrorException error) {
      errorRecordHandler.onError(
          new OnRecordErrorException(
              record,
              error.getErrorCode(),
              error.getParams()
          )
      );
    }
  }

  protected abstract Field processField(Record record, byte[] fieldData) throws OnRecordErrorException;
}
