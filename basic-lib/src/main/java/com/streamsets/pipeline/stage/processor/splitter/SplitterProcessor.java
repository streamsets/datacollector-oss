/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.splitter;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;

import java.util.List;

public class SplitterProcessor extends SingleLaneRecordProcessor {
  private final String fieldPath;
  private final char separator;
  private final List<String> fieldPathsForSplits;
  private final OnStagePreConditionFailure onStagePreConditionFailure;
  private final OriginalFieldAction originalFieldAction;

  public SplitterProcessor(String fieldPath, char separator, List<String> fieldPathsForSplits,
      OnStagePreConditionFailure onStagePreConditionFailure,
      OriginalFieldAction originalFieldAction) {
    this.fieldPath = fieldPath;
    this.separator = separator;
    this.fieldPathsForSplits = fieldPathsForSplits;
    this.onStagePreConditionFailure = onStagePreConditionFailure;
    this.originalFieldAction = originalFieldAction;
  }

  private boolean removeUnsplitValue;

  private String separatorStr;
  private String[] fieldPaths;

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    if (fieldPathsForSplits.size() < 2) {
      issues.add(getContext().createConfigIssue(Groups.FIELD_SPLITTER.name(), "fieldPathsForSplits",
                                                Errors.SPLITTER_00));
    }

    int count = 1;
    for(String fieldPath: fieldPathsForSplits) {
      if(fieldPath == null || fieldPath.trim().length() == 0) {
        issues.add(getContext().createConfigIssue(Groups.FIELD_SPLITTER.name(), "fieldPathsForSplits",
          Errors.SPLITTER_03, count));
      }
      count++;
    }

    if (issues.isEmpty()) {
      separatorStr = (separator == '^') ? " " : "" + separator;
      //forcing a fastpath for String.split()
      if (".$|()[{^?*+\\".contains(separatorStr)) {
        separatorStr = "\\" + separatorStr;
      }
      fieldPaths = fieldPathsForSplits.toArray(new String[fieldPathsForSplits.size()]);
      removeUnsplitValue = originalFieldAction == OriginalFieldAction.REMOVE;
    }
    return issues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Field field = record.get(fieldPath);
    String[] splits = null;
    ErrorCode error = null;
    if (field == null || field.getValue() == null) {
      error = Errors.SPLITTER_01;
    } else {
      String str;
      try {
        str = field.getValueAsString();
      } catch (IllegalArgumentException e) {
        throw new OnRecordErrorException(Errors.SPLITTER_04, fieldPath, field.getType().name());
      }
      splits = str.split(separatorStr, fieldPaths.length);
      if (splits.length < fieldPaths.length) {
        error = Errors.SPLITTER_02;
      }
    }
    if (error == null || onStagePreConditionFailure == OnStagePreConditionFailure.CONTINUE) {
      for (int i = 0; i < fieldPaths.length; i++) {
        try {
          if (splits != null && splits.length > i) {
            record.set(fieldPaths[i], Field.create(splits[i]));
          } else {
            record.set(fieldPaths[i], Field.create(Field.Type.STRING, null));
          }
        } catch (IllegalArgumentException e) {
          throw new OnRecordErrorException(Errors.SPLITTER_05, fieldPath, record.getHeader().getSourceId(),
            e.toString());
        }
      }
      if (removeUnsplitValue) {
        record.delete(fieldPath);
      }
      batchMaker.addRecord(record);
    } else {
      throw new OnRecordErrorException(error, record.getHeader().getSourceId(), fieldPath);
    }
  }

}
