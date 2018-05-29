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
package com.streamsets.pipeline.stage.processor.listpivot;

import com.google.api.client.util.Lists;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ListPivotProcessor extends SingleLaneRecordProcessor {

  private final boolean copyFields;
  private final boolean replaceListField;
  private final String listPath;
  private final String newPath;
  private final boolean saveOriginalFieldName;
  private final String originalFieldNamePath;
  private final OnStagePreConditionFailure onStagePreConditionFailure;

  public ListPivotProcessor(
      String listPath,
      String newPath,
      boolean copyFields,
      boolean replaceListField,
      boolean saveOriginalFieldName,
      String originalFieldNamePath,
      OnStagePreConditionFailure onStagePreConditionFailure
  ) {
    this.listPath = listPath;
    this.copyFields = copyFields;
    this.replaceListField = replaceListField;
    this.saveOriginalFieldName = saveOriginalFieldName;
    this.originalFieldNamePath = originalFieldNamePath;
    this.newPath = (StringUtils.isNotEmpty(newPath)) ? newPath : listPath;
    this.onStagePreConditionFailure = onStagePreConditionFailure;
  }

  private class PivotEntry {
    String name;
    Field value;

    PivotEntry(String name, Field value) {
      this.name = name;
      this.value = value;
    }
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    if (!copyFields && saveOriginalFieldName) {
      issues.add(getContext().createConfigIssue(Groups.PIVOT.name(), "copyFields", Errors.LIST_PIVOT_02));
    }

    if(!StringUtils.isEmpty(newPath) && newPath.equals(originalFieldNamePath)) {
      issues.add(getContext().createConfigIssue(Groups.PIVOT.name(), "originalFieldNamePath", Errors.LIST_PIVOT_03));
    }

    return issues;
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
    if (!listField.getType().isOneOf(Field.Type.LIST, Field.Type.LIST_MAP, Field.Type.MAP)) {
      throw new OnRecordErrorException(Errors.LIST_PIVOT_00, listPath);
    }

    List<PivotEntry> entries = Lists.newArrayList();
    if (listField.getType() == Field.Type.LIST) {
      List<Field> fieldList = listField.getValueAsList();
      for (Field field : fieldList) {
        entries.add(new PivotEntry(listPath, field));
      }
    } else {
      Set<Map.Entry<String, Field>> fieldEntries = listField.getValueAsMap().entrySet();
      for (Map.Entry<String, Field> entry : fieldEntries) {
        entries.add(new PivotEntry(entry.getKey(), entry.getValue()));
      }
    }

    pivot(entries, record, batchMaker);
  }

  private void pivot(List<PivotEntry> entries, Record record, SingleLaneBatchMaker batchMaker) {
    int i = 1;

    Record pivotRecord;
    if ((replaceListField) ||
        (newPath.equals(listPath))) {
      pivotRecord = getContext().cloneRecord(record);
      pivotRecord.delete(listPath);
    } else {
      pivotRecord = record;
    }

    for (PivotEntry fieldEntry : entries) {
      Record newRec;
      String sourceIdPostfix = i++ + "";
      newRec = getContext().cloneRecord(pivotRecord, sourceIdPostfix);

      if (copyFields) {
        newRec.set(newPath, fieldEntry.value);
      } else {
        newRec.set(fieldEntry.value);
      }

      if (saveOriginalFieldName) {
        newRec.set(originalFieldNamePath, Field.create(fieldEntry.name));
      }

      batchMaker.addRecord(newRec);
    }
  }
}
