/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.splitter;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
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
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();
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

    return issues;
  }

  @Override
  protected void init() throws StageException {
    super.init();

    separatorStr = (separator == '^') ? " " : "" + separator;

    //forcing a fastpath for String.split()
    if (".$|()[{^?*+\\".contains(separatorStr)) {
      separatorStr = "\\" + separatorStr;
    }

    fieldPaths = fieldPathsForSplits.toArray(new String[fieldPathsForSplits.size()]);

    removeUnsplitValue = originalFieldAction == OriginalFieldAction.REMOVE;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Field field = record.get(fieldPath);
    String[] splits = null;
    ErrorCode error = null;
    if (field == null || field.getValue() == null) {
      error = Errors.SPLITTER_01;
    } else {
      String str = field.getValueAsString();
      splits = str.split(separatorStr, fieldPaths.length);
      if (splits.length < fieldPaths.length) {
        error = Errors.SPLITTER_02;
      }
    }
    if (error == null || onStagePreConditionFailure == OnStagePreConditionFailure.CONTINUE) {
      for (int i = 0; i < fieldPaths.length; i++) {
        if (splits != null && splits.length > i) {
          record.set(fieldPaths[i], Field.create(splits[i]));
        } else {
          record.set(fieldPaths[i], Field.create(Field.Type.STRING, null));
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
