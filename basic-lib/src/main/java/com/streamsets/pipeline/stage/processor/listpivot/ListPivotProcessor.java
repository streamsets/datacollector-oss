/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.processor.listpivot;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;

import java.util.List;

public class ListPivotProcessor extends SingleLaneRecordProcessor {

  private final boolean copyFields;
  private final String listPath;
  private final OnStagePreConditionFailure onStagePreConditionFailure;

  public ListPivotProcessor(
      String listPath,
      boolean copyFields,
      OnStagePreConditionFailure onStagePreConditionFailure
  ) {
    this.listPath = listPath;
    this.copyFields = copyFields;
    this.onStagePreConditionFailure = onStagePreConditionFailure;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    if (!record.has(listPath)) {
      switch (onStagePreConditionFailure) {
        case TO_ERROR:
          throw new OnRecordErrorException(Errors.LIST_PIVOT_01, record.getHeader().getSourceId(), listPath);
        case CONTINUE:
          return;
        default:
          throw new IllegalStateException("Invalid value for on stage pre-condition failure");
      }
    }

    Field listField = record.get(listPath);
    if (listField.getType() != Field.Type.LIST) {
      throw new OnRecordErrorException(Errors.LIST_PIVOT_00, listPath);
    }

    List<Field> list = listField.getValueAsList();
    for (Field field : list) {
      Record newRec;
      if (copyFields) {
        newRec = getContext().cloneRecord(record);
        newRec.set(listPath, field);
      } else {
        newRec = getContext().createRecord(record);
        newRec.set(field);
      }
      batchMaker.addRecord(newRec);
    }
  }
}
