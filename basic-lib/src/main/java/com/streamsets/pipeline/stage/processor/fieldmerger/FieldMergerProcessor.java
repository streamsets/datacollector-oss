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
package com.streamsets.pipeline.stage.processor.fieldmerger;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Processor for merging list and map fields.
 *
 * Semantics are as follows:
 *   1. If source field does not exist, success of processor depends on the
 *      precondition-failure configuration.
 *   2. If source and target field types do not match, processor fails.
 *   3. If source and target fields are maps, the fields within the source map will be added
 *      to the target map. The source map will be removed. If nested fields have conflicting
 *      names, the success of the processor will depend on the overwrite configuration.
 *   4. If the source and target fields are lists, the elements of the source list will be
 *      concatenated to the end of the target list. There is no possibility of overwrite
 *      in this case.
 */
public class FieldMergerProcessor extends SingleLaneRecordProcessor {
  private final List<FieldMergerConfig> mergeMapping;
  private final OnStagePreConditionFailure onStagePreConditionFailure;
  private final boolean overwriteExisting;

  private final static String MAP_FIELD_SEPARATOR = "/";

  public FieldMergerProcessor(List<FieldMergerConfig> mergeMapping,
                              OnStagePreConditionFailure onStagePreConditionFailure,
                              boolean overwriteExisting) {
    this.mergeMapping = mergeMapping;
    this.onStagePreConditionFailure = onStagePreConditionFailure;
    this.overwriteExisting = overwriteExisting;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Set<String> fieldsThatDoNotExist = new HashSet<>();
    Set<String> fieldsRequiringOverwrite = new HashSet<>();
    for (FieldMergerConfig mergeConfig : mergeMapping) {
      String fromFieldName = mergeConfig.fromField;
      String toFieldName = mergeConfig.toField;

      if (!record.has(fromFieldName)) {
        // Neither field exists, so generate an error for non-existent source field
        fieldsThatDoNotExist.add(fromFieldName);
      } else if (!record.has(toFieldName)) {
        // Easy case, which is a straight rename
        Field fromField = record.get(fromFieldName);
        if (isListOrMapField(fromField)) {
          try {
            record.set(toFieldName, fromField);
            record.delete(fromFieldName);
          } catch (IllegalArgumentException e) {
            throw new OnRecordErrorException(Errors.FIELD_MERGER_04);
          }
        } else {
          throw new OnRecordErrorException(Errors.FIELD_MERGER_03);
        }
      } else {
        // Both fields exist, and we can try to merge
        Field fromField = record.get(fromFieldName);
        Field toField = record.get(toFieldName);

        checkValidTypes(fromField, toField);
        if (fromField.getType().isOneOf(Field.Type.MAP, Field.Type.LIST_MAP)) {
          // Type is MAP
          Map<String, Field> map = fromField.getValueAsMap();
          for (Map.Entry<String, Field> entry : map.entrySet()) {
            // If toFieldName is the root field, don't add an extra separator
            String newPath = (toFieldName.equals(MAP_FIELD_SEPARATOR) ? "" : toFieldName)+
                MAP_FIELD_SEPARATOR + entry.getKey();
            if (record.has(newPath) && !overwriteExisting) {
              fieldsRequiringOverwrite.add(toFieldName);
            } else {
              try {
                record.set(newPath, entry.getValue());
              } catch (IllegalArgumentException e) {
                throw new OnRecordErrorException(Errors.FIELD_MERGER_04);
              }
            }
          }
        } else {
          // Type is LIST
          List<Field> list = toField.getValueAsList();
          list.addAll(fromField.getValueAsList());
          try {
            record.set(toFieldName, Field.create(list));
          } catch (IllegalArgumentException e) {
            throw new OnRecordErrorException(Errors.FIELD_MERGER_04);
          }
        }

        record.delete(fromFieldName);
      }
    }

    if (onStagePreConditionFailure == OnStagePreConditionFailure.TO_ERROR && !fieldsThatDoNotExist.isEmpty()) {
     throw new OnRecordErrorException(Errors.FIELD_MERGER_00, record.getHeader().getSourceId(),
       Joiner.on(", ").join(fieldsThatDoNotExist));
    }

    if (!overwriteExisting && !fieldsRequiringOverwrite.isEmpty()) {
      throw new OnRecordErrorException(Errors.FIELD_MERGER_01,
          Joiner.on(", ").join(fieldsRequiringOverwrite),
          record.getHeader().getSourceId());
    }
    batchMaker.addRecord(record);
  }

  private void checkValidTypes(Field fromField, Field toField) throws OnRecordErrorException {
    if (!fromField.getType().equals(toField.getType())) {
      throw new OnRecordErrorException(Errors.FIELD_MERGER_02,
          fromField.getType().name(), toField.getType().name());
    }

    if (!isListOrMapField(fromField)) {
      throw new OnRecordErrorException(Errors.FIELD_MERGER_02,
          fromField.getType().name(), toField.getType().name());
    }
  }

  private boolean isListOrMapField(Field field) {
    return field.getType().isOneOf(Field.Type.MAP, Field.Type.LIST, Field.Type.LIST_MAP);
  }
}
