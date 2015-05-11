package com.streamsets.pipeline.stage.processor.javascript;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.stage.processor.scripting.ScriptObjectFactory;

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



  public Java8JavaScriptObjectFactory(ScriptEngine engine) {
    super(engine);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Field scriptToField(Object scriptObject) {
    Field field;
    if (scriptObject != null) {
      if (SCRIPT_OBJECT_MIRROR_CLASS.isInstance(scriptObject)) {
        try {
          Set set = (Set) ENTRY_SET_METHOD.invoke(scriptObject);
          if ((boolean) IS_ARRAY_METHOD.invoke(scriptObject)) {
            List<Field> fields = new ArrayList<>(set.size());
            for (Object obj : set) {
              Map.Entry<String, Object> entry = (Map.Entry<String, Object>) obj;
              fields.add(scriptToField(entry.getValue()));
            }
            field = Field.create(fields);
          } else {
            Map<String, Field> fields = new LinkedHashMap<>();
            for (Object obj : set) {
              Map.Entry<String, Object> entry = (Map.Entry<String, Object>) obj;
              fields.put(entry.getKey(), scriptToField(entry.getValue()));
            }
            field = Field.create(fields);
          }
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      } else if(scriptObject instanceof Map) {
        Map mapScriptObject = (Map)scriptObject;
        Map<String, Field> fields = new LinkedHashMap<>();
        for (Object key : mapScriptObject.keySet()) {
          fields.put(key.toString(), scriptToField(mapScriptObject.get(key)));
        }
        field = Field.create(fields);
      } else if(scriptObject instanceof List) {
        List listScriptObject = (List)scriptObject;
        List<Field> fields = new ArrayList<>(listScriptObject.size());
        for (Object listObj : listScriptObject) {
          fields.add(scriptToField(listObj));
        }
        field = Field.create(fields);
      } else {
        field = convertPrimitiveObject(scriptObject);
      }
    } else {
      field = Field.create((String)null);
    }
    return field;
  }
}