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
package com.streamsets.pipeline.stage.processor.javascript;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.util.scripting.ScriptObjectFactory;
import com.streamsets.pipeline.stage.util.scripting.config.ScriptRecordType;

import javax.script.ScriptEngine;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Java8JavaScriptObjectFactory extends ScriptObjectFactory {

  private static final Class SCRIPT_OBJECT_MIRROR_CLASS;
  private static final Method IS_ARRAY_METHOD;
  private static final Method ENTRY_SET_METHOD;
  private static final String REFLECTION_ERROR_MESSAGE = "Error performing reflection on " +
                                                         System.getProperty("java.version") + ". Please report: ";
  static {
    try {
      SCRIPT_OBJECT_MIRROR_CLASS = ClassLoader.getSystemClassLoader().loadClass(
          "jdk.nashorn.api.scripting.ScriptObjectMirror");
      //noinspection unchecked
      IS_ARRAY_METHOD = SCRIPT_OBJECT_MIRROR_CLASS.getMethod("isArray");
      //noinspection unchecked
      ENTRY_SET_METHOD = SCRIPT_OBJECT_MIRROR_CLASS.getMethod("entrySet");

    } catch (Exception ex) {
      throw new RuntimeException(REFLECTION_ERROR_MESSAGE + ex, ex);
    }
  }



  public Java8JavaScriptObjectFactory(ScriptEngine engine, Stage.Context context, ScriptRecordType scriptRecordType) {
    super(engine, context, scriptRecordType);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Field scriptToField(Object scriptObject, Record record, String path) {
    Field field;
    if (scriptObject != null) {
      if (SCRIPT_OBJECT_MIRROR_CLASS.isInstance(scriptObject)) {
        try {
          Set set = (Set) ENTRY_SET_METHOD.invoke(scriptObject);
          if ((boolean) IS_ARRAY_METHOD.invoke(scriptObject)) {
            List<Field> fields = new ArrayList<>(set.size());
            for (Object obj : set) {
              Map.Entry<String, Object> entry = (Map.Entry<String, Object>) obj;
              fields.add(scriptToField(entry.getValue(), record, composeMapPath(path,entry.getKey())));
            }
            field = Field.create(fields);
          } else {
            Map<String, Field> fields = new LinkedHashMap<>();
            for (Object obj : set) {
              Map.Entry<String, Object> entry = (Map.Entry<String, Object>) obj;
              fields.put(
                  entry.getKey(),
                  scriptToField(entry.getValue(),
                      record,
                      composeMapPath(path, entry.getKey())
                  )
              );
            }
            field = Field.create(fields);
          }
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      } else if(scriptObject instanceof Map) {
        Map<Object, Object> mapScriptObject = (Map<Object,Object>)scriptObject;
        LinkedHashMap<String, Field> fieldMap = new LinkedHashMap<>();
        for (Map.Entry entry : mapScriptObject.entrySet()) {
          fieldMap.put(
              entry.getKey().toString(),
              scriptToField(
                  mapScriptObject.get(entry.getKey()),
                  record,
                  composeMapPath(path, entry.getKey().toString())
              )
          );
        }
        boolean isListMap = (scriptObject instanceof MapInfo) && ((MapInfo) scriptObject).isListMap();
        field = (isListMap) ? Field.createListMap(fieldMap) : Field.create(fieldMap);
      } else if(scriptObject instanceof List) {
        List listScriptObject = (List)scriptObject;
        List<Field> fields = new ArrayList<>(listScriptObject.size());
        for (int i = 0; i < listScriptObject.size(); i++) {
          Object listObj = listScriptObject.get(i);
          fields.add(scriptToField(listObj, record, composeArrayPath(path, i)));
        }
        field = Field.create(fields);
      } else {
        field = convertPrimitiveObject(scriptObject);
      }
    } else {
      Field originalField = record.get(path);
      if (originalField != null) {
        field = Field.create(originalField.getType(), null);
      } else {
        field = Field.create((String) null);
      }
    }
    return field;
  }
}
