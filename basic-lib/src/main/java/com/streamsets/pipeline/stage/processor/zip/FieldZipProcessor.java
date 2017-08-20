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
package com.streamsets.pipeline.stage.processor.zip;

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FieldZipProcessor extends SingleLaneRecordProcessor {

  private final List<FieldZipConfig> fieldZipConfigs;
  private final boolean valuesOnly;
  private final OnStagePreConditionFailure onStagePreConditionFailure;

  public FieldZipProcessor(
      FieldZipConfigBean configBean
  ) {
    this.fieldZipConfigs = configBean.fieldZipConfigs;
    this.valuesOnly = configBean.valuesOnly;
    this.onStagePreConditionFailure = configBean.onStagePreConditionFailure;
  }

  private class ZipEntry {
    String name;
    Field value;

    ZipEntry(String name, Field value) {
      this.name = name;
      this.value = value;
    }
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    // If the record does not correspond to configuration either throw an exception or return and do not continue
    if(!checkConfigs(record)) {
      batchMaker.addRecord(record);
      return;
    }


    for (FieldZipConfig zipConfig : fieldZipConfigs) {
      String firstFieldPath = zipConfig.firstField;
      String secondFieldPath = zipConfig.secondField;
      String zippedPath = zipConfig.zippedFieldPath;

      List<ZipEntry> firstEntries = getZipEntries(firstFieldPath, record.get(firstFieldPath));
      List<ZipEntry> secondEntries = getZipEntries(secondFieldPath, record.get(secondFieldPath));
      List<Field> zipped = Lists.newArrayList();
      if (valuesOnly) {
        for (int i = 0; i < firstEntries.size() && i < secondEntries.size(); i++) {
          zipped.add(Field.create(Arrays.asList(firstEntries.get(i).value, secondEntries.get(i).value)));
        }
      } else {
        for (int i = 0; i < firstEntries.size() && i < secondEntries.size(); i++) {
          zipped.add(Field.create(ImmutableMap.of(firstEntries.get(i).name, firstEntries.get(i).value,
              secondEntries.get(i).name, secondEntries.get(i).value)));
        }
      }

      record.set(zippedPath, Field.create(zipped));
    }

    batchMaker.addRecord(record);
  }

  private List<ZipEntry> getZipEntries(String fieldPath, Field field) {
    List<ZipEntry> zipEntries = Lists.newArrayList();
    if (field.getType() == Field.Type.LIST) {
      String fieldName = fieldPath.substring(fieldPath.lastIndexOf("/") + 1);
      for (Field entry : field.getValueAsList()) {
        zipEntries.add(new ZipEntry(fieldName, entry));
      }
    } else if (field.getType().isOneOf(Field.Type.LIST_MAP, Field.Type.MAP)) {
      for (Map.Entry<String, Field> entry : field.getValueAsMap().entrySet()) {
        zipEntries.add(new ZipEntry(entry.getKey(), entry.getValue()));
      }
    }
    return zipEntries;
  }

  private boolean checkConfigs(Record record) throws OnRecordErrorException {
    for (FieldZipConfig zipConfig : fieldZipConfigs) {
      List<String> missingFields = Lists.newArrayList();
      List<String> nonListFields = Lists.newArrayList();

      if (!record.has(zipConfig.firstField)) {
        missingFields.add(zipConfig.firstField);
      } else if (!record.get(zipConfig.firstField).getType().isOneOf(Field.Type.LIST, Field.Type.LIST_MAP)) {
        nonListFields.add(zipConfig.firstField);
      }
      if (!record.has(zipConfig.secondField)) {
        missingFields.add(zipConfig.secondField);
      } else if (!record.get(zipConfig.secondField).getType().isOneOf(Field.Type.LIST, Field.Type.LIST_MAP)) {
        nonListFields.add(zipConfig.secondField);
      }

      switch (onStagePreConditionFailure) {
        case TO_ERROR:
          if (!missingFields.isEmpty()) {
            throw new OnRecordErrorException(Errors.ZIP_01, missingFields);
          } else if (!nonListFields.isEmpty()) {
            throw new OnRecordErrorException(Errors.ZIP_00, nonListFields);
          }
          break;
        case CONTINUE:
          if(!missingFields.isEmpty() || !nonListFields.isEmpty()) {
            return false;
          }
          break;
        default:
          throw new IllegalStateException("Invalid value for on stage pre-condition failure");
      }
    }

    return true;
  }
}
