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
package com.streamsets.pipeline.stage.processor.fieldflattener;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
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

    // Process the root field and create a new root Map
    Map<String, Field> newRoot;
    switch (config.flattenType) {
      case ENTIRE_RECORD:
        newRoot = flattenEntireRecord(rootField);
        break;
      default:
        throw new IllegalArgumentException("Unknown flatten type: " + config.flattenType);
    }

    // Propagate flattened record through
    record.set(Field.create(Field.Type.MAP, newRoot));
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
