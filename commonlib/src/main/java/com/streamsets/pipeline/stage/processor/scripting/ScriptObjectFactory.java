/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.processor.scripting;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;

import javax.script.ScriptEngine;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ScriptObjectFactory {

  protected final ScriptEngine engine;

  public ScriptObjectFactory(ScriptEngine engine) {
    this.engine = engine;
  }

  public ScriptRecord createScriptRecord(Record record) {
    Object scriptValue = null;
    if (record.get() != null) {
      scriptValue = fieldToScript(record.get());
    }
    return new ScriptRecord(record, scriptValue);
  }

  @SuppressWarnings("unchecked")
  public Record getRecord(ScriptRecord scriptRecord) {
    Record record = scriptRecord.record;
    Field field = scriptToField(scriptRecord.value);
    record.set(field);
    return record;
  }

  @SuppressWarnings("unchecked")
  public void putInMap(Object obj, Object key, Object value) {
    ((Map)obj).put(key, value);
  }

  public Object createMap() {
    return new LinkedHashMap<>();
  }

  public Object createArray(List elements) {
    return elements;
  }

  @SuppressWarnings("unchecked")
  protected Object fieldToScript(Field field) {
    Object scriptObject = null;
    if (field != null) {
      scriptObject = field.getValue();
      if (scriptObject != null) {
        switch (field.getType()) {
          case MAP:
            Map<String, Field> fieldMap = (Map<String, Field>) scriptObject;
            Object scriptMap = createMap();
            for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
              putInMap(scriptMap, entry.getKey(), fieldToScript(entry.getValue()));
            }
            scriptObject = scriptMap;
            break;
          case LIST:
            List<Field> fieldArray = (List<Field>) scriptObject;
            List scripArrayElements = new ArrayList();
            for (Field aFieldArray : fieldArray) {
              scripArrayElements.add(fieldToScript(aFieldArray));
            }
            scriptObject = createArray(scripArrayElements);
            break;
        }
      }
    }
    return scriptObject;
  }

  @SuppressWarnings("unchecked")
  protected Field scriptToField(Object scriptObject) {
    Field field;
    if (scriptObject != null) {
      if (scriptObject instanceof Map) {
        Map<String, Object> scriptMap = (Map<String, Object>) scriptObject;
        Map<String, Field> fieldMap = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : scriptMap.entrySet()) {
          fieldMap.put(entry.getKey(), scriptToField(entry.getValue()));
        }
        field = Field.create(fieldMap);
      } else if (scriptObject instanceof List) {
        List scriptArray = (List) scriptObject;
        List<Field> fieldArray = new ArrayList<>(scriptArray.size());
        for (Object element : scriptArray) {
          fieldArray.add(scriptToField(element));
        }
        field = Field.create(fieldArray);
      } else {
        field = convertPrimitiveObject(scriptObject);
      }
    } else {
      field = Field.create((String)null);
    }
    return field;
  }

  protected Field convertPrimitiveObject(Object scriptObject) {
    Field field;
    if (scriptObject instanceof Boolean) {
      field = Field.create((Boolean) scriptObject);
    } else if (scriptObject instanceof Character) {
      field = Field.create((Character) scriptObject);
    } else if (scriptObject instanceof Byte) {
      field = Field.create((Byte) scriptObject);
    } else if (scriptObject instanceof Short) {
      field = Field.create((Short) scriptObject);
    } else if (scriptObject instanceof Integer) {
      field = Field.create((Integer) scriptObject);
    } else if (scriptObject instanceof Long) {
      field = Field.create((Long) scriptObject);
    } else if (scriptObject instanceof Float) {
      field = Field.create((Float) scriptObject);
    } else if (scriptObject instanceof Double) {
      field = Field.create((Double) scriptObject);
    } else if (scriptObject instanceof Date) {
      field = Field.createDate((Date) scriptObject);
    } else if (scriptObject instanceof BigDecimal) {
      field = Field.create((BigDecimal) scriptObject);
    } else if (scriptObject instanceof String) {
      field = Field.create((String) scriptObject);
    } else if (scriptObject instanceof byte[]) {
      field = Field.create((byte[]) scriptObject);
    } else {
      field = Field.create(scriptObject.toString());
    }
    return field;
  }

}
