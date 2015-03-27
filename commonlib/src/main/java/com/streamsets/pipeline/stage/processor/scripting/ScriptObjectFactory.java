/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.scripting;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ScriptObjectFactory {

  public Object createRecord(Record record) {
    Object scriptObject = createMap();
    setRecordInternal(scriptObject, record);
    if (record.get() != null) {
      setField(scriptObject, fieldToScript(record.get()));
    }
    return scriptObject;
  }

  @SuppressWarnings("unchecked")
  public Record getRecord(Object scriptRecord) {
    Record record = getRecordInternal(scriptRecord);
    Field field = scriptToField((Map)scriptRecord, true);
    record.set(field);
    return record;
  }

  @SuppressWarnings("unchecked")
  public void putInMap(Object obj, String key, Object value) {
    ((Map)obj).put(key, value);
  }

  public Object createMap() {
    return new LinkedHashMap<>();
  }

  public Object createArray(List elements) {
    return elements;
  }

  protected Record getRecordInternal(Object scriptRecord) {
    return (Record) ((Map)scriptRecord).get("_record");
  }

  @SuppressWarnings("unchecked")
  protected void setRecordInternal(Object scriptRecord, Record record) {
    putInMap(scriptRecord, "_record", record);
  }

  @SuppressWarnings("unchecked")
  protected void setField(Object scriptRecord, Object scriptField) {
    if (scriptField != null) {
      for (Map.Entry<String, Object> entry : ((Map<String, Object>)scriptField).entrySet()) {
        putInMap(scriptRecord, entry.getKey(), entry.getValue());
      }
    }
  }

  @SuppressWarnings("unchecked")
  protected Object fieldToScript(Field field) {
    Object scriptObj = createMap();
    if (field != null) {
      putInMap(scriptObj, "type", field.getType());
      Object value = field.getValue();
      if (value != null) {
        switch (field.getType()) {
          case MAP:
            Map<String, Field> fieldMap = (Map<String, Field>) value;
            Object scriptMap = createMap();
            for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
              putInMap(scriptMap, entry.getKey(), fieldToScript(entry.getValue()));
            }
            value = scriptMap;
            break;
          case LIST:
            List<Field> fieldArray = (List<Field>) value;
            List scripArrayElements = new ArrayList();
            for (Field aFieldArray : fieldArray) {
              scripArrayElements.add(fieldToScript(aFieldArray));
            }
            value = createArray(scripArrayElements);
            break;
        }
      }
      putInMap(scriptObj, "value", value);
    }
    return scriptObj;
  }

  @SuppressWarnings("unchecked")
  protected Field scriptToField(Map<String, Object> map, boolean root) {
    Field field = null;
    if (map != null) {
      if (!root || map.containsKey("type")) {
        Field.Type type = (Field.Type) map.get("type");
        Object value = map.get("value");
        if (value != null) {
          switch (type) {
            case MAP:
              Map<String, Object> scriptMap = (Map<String, Object>) value;
              Map<String, Field> fieldMap = new LinkedHashMap<>();
              for (Map.Entry<String, Object> entry : scriptMap.entrySet()) {
                fieldMap.put(entry.getKey(), scriptToField((Map<String, Object>) entry.getValue(), false));
              }
              value = fieldMap;
              break;
            case LIST:
              List scriptArray = (List) value;
              List<Field> fieldArray = new ArrayList<>(scriptArray.size());
              for (Object element : scriptArray) {
                fieldArray.add(scriptToField((Map)element, false));
              }
              value = fieldArray;
              break;
          }
        }
        field = Field.create(type, value);
      }
    }
    return field;
  }

}
