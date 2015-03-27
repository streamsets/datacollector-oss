/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.javascript;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.AbstractScriptingProcessor;
import com.streamsets.pipeline.stage.processor.scripting.ScriptObjectFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public class JavaScriptProcessor extends AbstractScriptingProcessor {

  public static final String JAVASCRIPT_ENGINE = "rhino";

  public JavaScriptProcessor(ProcessingMode processingMode, String script) {
    super(JAVASCRIPT_ENGINE, Groups.JAVASCRIPT.name(), "script", processingMode, script);
  }

  private static final Class MAP_OBJECT_CLASS;
  private static final Method MAP_PUT_METHOD;
  private static final int MAP_PUT_MODE_PERMANENT;
  private static final int MAP_PUT_MODE_READONLY;
  private static final Constructor ARRAY_CONSTRUCTOR;

  // we are doing this reflection voodoo because the Java compile does not let you use sun....internal packages
  // PRRRRRRRRHHHHHHHHHHH Oracle.
  static {
    try {
      MAP_OBJECT_CLASS = ClassLoader.getSystemClassLoader().loadClass(
          "sun.org.mozilla.javascript.internal.NativeObject");
      //noinspection unchecked
      MAP_PUT_METHOD = MAP_OBJECT_CLASS.getMethod("defineProperty", String.class, Object.class, Integer.TYPE);
      MAP_PUT_MODE_PERMANENT = MAP_OBJECT_CLASS.getField("PERMANENT").getInt(null);
      MAP_PUT_MODE_READONLY = MAP_OBJECT_CLASS.getField("READONLY").getInt(null);

      Class arrayClass = ClassLoader.getSystemClassLoader().loadClass(
          "sun.org.mozilla.javascript.internal.NativeArray");
      //noinspection unchecked
      ARRAY_CONSTRUCTOR = arrayClass.getConstructor(Object[].class);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private static Object createMapByReflection() {
    try {
      return MAP_OBJECT_CLASS.newInstance();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private static Object createArrayByReflection(Object[] elements) {
    try {
      return ARRAY_CONSTRUCTOR.newInstance(elements);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private static void definePropertyByReflection(Object obj, String key, Object value, int mode) {
    try {
      MAP_PUT_METHOD.invoke(obj, key, value, mode);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  protected ScriptObjectFactory getScriptObjectFactory() {
    return new ScriptObjectFactory() {

      @Override
      public void putInMap(Object obj, String key, Object value) {
        definePropertyByReflection(obj, key, value, MAP_PUT_MODE_PERMANENT);
      }

      @Override
      public Object createMap() {
        return createMapByReflection();
      }

      @Override
      public Object createArray(List elements) {
        return createArrayByReflection(elements.toArray(new Object[elements.size()]));
      }

      @Override
      protected void setField(Object scriptRecord, Object scriptField) {
        super.setField(scriptRecord, scriptField);
      }

      @Override
      protected Object fieldToScript(Field field) {
        return super.fieldToScript(field);
      }

      @Override
      protected Field scriptToField(Map<String, Object> map) {
        return super.scriptToField(map);
      }

      @Override
      protected Record getRecordInternal(Object scriptRecord) {
        return (Record) ((Map)scriptRecord).get("_record");
      }

      @Override
      protected void setRecordInternal(Object scriptRecord, Record record) {
        definePropertyByReflection(scriptRecord, "_record", record, MAP_PUT_MODE_READONLY);
      }

    };
  }

  @Override
  protected Object createScriptType() {
    ScriptObjectFactory sof = getScriptObjectFactory();
    Object scriptMap = sof.createMap();
    for (Field.Type type : Field.Type.values()) {
      sof.putInMap(scriptMap, type.name(), type);
    }
    return scriptMap;
  }

}
