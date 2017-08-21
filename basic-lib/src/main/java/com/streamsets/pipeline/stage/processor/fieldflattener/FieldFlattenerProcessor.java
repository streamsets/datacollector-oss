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
package com.streamsets.pipeline.stage.processor.fieldflattener;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FieldFlattenerProcessor extends SingleLaneRecordProcessor {

  private FieldFlattenerConfig config;

  public FieldFlattenerProcessor(FieldFlattenerConfig config) {
    this.config = config;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
    // Shortcut for case when root item is not a complex type - we just return whatever was there before
    Field rootField = record.get();
    if(rootField == null || !rootField.getType().isOneOf(Field.Type.MAP, Field.Type.LIST_MAP, Field.Type.LIST)) {
      singleLaneBatchMaker.addRecord(record);
      return;
    }

    switch (config.flattenType) {
      case ENTIRE_RECORD:
        // Process the root field and create a new root Map
        final Map<String, Field> newRoot = flattenEntireRecord(rootField);
        // Propagate flattened record through
        record.set(Field.create(Field.Type.MAP, newRoot));
        break;
      case SPECIFIC_FIELDS:
        flattenSpecificFields(record);
        break;
      default:
        throw new IllegalArgumentException("Unknown flatten type: " + config.flattenType);
    }
    singleLaneBatchMaker.addRecord(record);

  }

  // Flatten the entire record to one giant map
  private Map<String, Field> flattenEntireRecord(Field rootField) {
    Map<String, Field> ret = new LinkedHashMap<>();
    switch (rootField.getType()) {
      case MAP:
      case LIST_MAP:
        flattenMap("", rootField.getValueAsMap(), ret);
        break;
      case LIST:
        flattenList("", rootField.getValueAsList(), ret);
        break;
      default:
        break;
    }

    return ret;
  }

  private void flattenSpecificFields(Record record) throws OnRecordErrorException {
    Field flattenTarget = null;

    if(!config.flattenInPlace) {
      if(!record.has(config.flattenTargetField)) {
        throw new OnRecordErrorException(record, Errors.FIELD_FLATTENER_02, config.flattenTargetField);
      }

      flattenTarget = record.get(config.flattenTargetField);
      if(!flattenTarget.getType().isOneOf(Field.Type.MAP, Field.Type.LIST_MAP)) {
        throw new OnRecordErrorException(record, Errors.FIELD_FLATTENER_03, config.flattenTargetField, flattenTarget.getType().name());
      }
    }

    for (String flattenField : config.fields) {
      if (record.has(flattenField)) {
        final Map<String, Field> flattened = flattenEntireRecord(record.get(flattenField));
        if(config.flattenInPlace) {
          record.set(flattenField, Field.create(Field.Type.MAP, flattened));
        } else {
          appendFieldsToRecord(flattened, record, flattenTarget);

          if(config.removeFlattenedField) {
            record.delete(flattenField);
          }
        }
      } else {
        throw new OnRecordErrorException(record, Errors.FIELD_FLATTENER_01, flattenField);
      }
    }
  }

  private void appendFieldsToRecord(Map<String, Field> flattened, Record record, Field flattenTarget) throws OnRecordErrorException {
    Map<String, Field> flattenTargetMap = flattenTarget.getValueAsMap();

    for(Map.Entry<String, Field> entry : flattened.entrySet()) {
      if(flattenTargetMap.containsKey(entry.getKey())) {
        switch (config.collisionFieldAction) {
          case DISCARD:
            continue; // Simply continue in the loop
          case TO_ERROR:
            throw new OnRecordErrorException(record, Errors.FIELD_FLATTENER_04, entry.getKey());
          case OVERRIDE:
            // Jump out of the switch and replace the field:
        }
      }

      flattenTarget.getValueAsMap().put(entry.getKey(), entry.getValue());
    }
  }

  private void flattenField(String prefix, Field field, Map<String, Field> ret) {
     switch (field.getType()) {
      case MAP:
      case LIST_MAP:
        flattenMap(prefix, field.getValueAsMap(), ret);
        break;
      case LIST:
        flattenList(prefix, field.getValueAsList(), ret);
        break;
      default:
        ret.put(prefix, field);
    }
  }

  private void flattenMap(String prefix, Map<String, Field> valueAsMap, Map<String, Field> ret) {
    if(valueAsMap == null) {
      return;
    }

    for(Map.Entry<String, Field> entry : valueAsMap.entrySet()) {
      String name = entry.getKey();
      Field value = entry.getValue();
      flattenField(generatedName(prefix, name), value, ret);
    }
  }

  private void flattenList(String prefix, List<Field> valueAsList, Map<String, Field> ret) {
    int i = 0;
    for(Field value : valueAsList) {
      flattenField(generatedName(prefix, i++), value, ret);
    }
  }

  private String generatedName(String prefix, int index) {
    return generatedName(prefix, "" + index);
  }

  private String generatedName(String prefix, String newComponent) {
    if(prefix.isEmpty()) {
      return newComponent;
    } else {
      return prefix + config.nameSeparator + newComponent;
    }
  }

}
