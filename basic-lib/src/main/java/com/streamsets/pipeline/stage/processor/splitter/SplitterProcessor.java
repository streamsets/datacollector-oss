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
package com.streamsets.pipeline.stage.processor.splitter;

import com.google.api.client.util.Lists;
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
  private final String separator;
  private final List<String> fieldPathsForSplits;
  private final TooManySplitsAction tooManySplitsAction;
  private final String remainingSplitsPath;
  private final OnStagePreConditionFailure onStagePreConditionFailure;
  private final OriginalFieldAction originalFieldAction;

  public SplitterProcessor(String fieldPath, String separator, List<String> fieldPathsForSplits,
      TooManySplitsAction tooManySplitsAction,
      String remainingSplitsPath,
      OnStagePreConditionFailure onStagePreConditionFailure,
      OriginalFieldAction originalFieldAction) {
    this.fieldPath = fieldPath;
    this.separator = separator;
    this.fieldPathsForSplits = fieldPathsForSplits;
    this.tooManySplitsAction = tooManySplitsAction;
    this.remainingSplitsPath = remainingSplitsPath;
    this.onStagePreConditionFailure = onStagePreConditionFailure;
    this.originalFieldAction = originalFieldAction;
  }

  private boolean removeUnsplitValue;

  private String[] fieldPaths;

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    int count = 1;
    for(String fieldPath: fieldPathsForSplits) {
      if(fieldPath == null || fieldPath.trim().length() == 0) {
        issues.add(getContext().createConfigIssue(Groups.FIELD_SPLITTER.name(), "fieldPathsForSplits",
          Errors.SPLITTER_03, count));
      }
      count++;
    }

    if (fieldPathsForSplits.size() == 0 && tooManySplitsAction == TooManySplitsAction.TO_LAST_FIELD) {
      issues.add(getContext().createConfigIssue(Groups.FIELD_SPLITTER.name(), "fieldPathsForSplits",
          Errors.SPLITTER_00));
    }

    if (issues.isEmpty()) {
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

      switch (tooManySplitsAction) {
        case TO_LAST_FIELD:
          splits = str.split(separator, fieldPaths.length);
          break;
        case TO_LIST:
          splits = str.split(separator);
          break;
        default:
          throw new IllegalArgumentException("Unsupported value for too many splits action: " + tooManySplitsAction);
      }

      if (splits.length < fieldPaths.length) {
        error = Errors.SPLITTER_02;
      }
    }
    if (error == null || onStagePreConditionFailure == OnStagePreConditionFailure.CONTINUE) {
      int i = 0;
      for (i = 0; i < fieldPaths.length; i++) {
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

      if (splits != null && i < splits.length && tooManySplitsAction == TooManySplitsAction.TO_LIST) {
        List<Field> remainingSplits = Lists.newArrayList();
        for (int j = i; j < splits.length; j++) {
          remainingSplits.add(Field.create(splits[j]));
        }
        record.set(remainingSplitsPath, Field.create(remainingSplits));
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
