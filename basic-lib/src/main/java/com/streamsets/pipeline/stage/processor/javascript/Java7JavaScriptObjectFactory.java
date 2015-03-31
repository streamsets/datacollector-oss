package com.streamsets.pipeline.stage.processor.javascript;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.stage.processor.scripting.ScriptObjectFactory;

import javax.script.ScriptEngine;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public class Java7JavaScriptObjectFactory extends ScriptObjectFactory {
  private static final Class MAP_OBJECT_CLASS;
  private static final Method MAP_PUT_METHOD;
  private static final int MAP_PUT_MODE_PERMANENT;
  private static final int MAP_PUT_MODE_READONLY;
  private static final Constructor ARRAY_CONSTRUCTOR;
  private static final String REFLECTION_ERROR_MESSAGE = "Error performing reflection on " +
    System.getProperty("java.version") + ". Please report: ";
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
      throw new RuntimeException(REFLECTION_ERROR_MESSAGE + ex, ex);
    }
  }
  public Java7JavaScriptObjectFactory(ScriptEngine engine) {
    super(engine);
  }
  @Override
  public Object createArray(List elements) {
    try {
      return ARRAY_CONSTRUCTOR.newInstance(elements.toArray(new Object[elements.size()]));
    } catch (Exception ex) {
      throw new RuntimeException(REFLECTION_ERROR_MESSAGE + ex, ex);
    }
  }

  @Override
  public Object createMap() {
    try {
      return MAP_OBJECT_CLASS.newInstance();
    } catch (Exception ex) {
      throw new RuntimeException(REFLECTION_ERROR_MESSAGE + ex, ex);
    }
  }

  @Override
  protected void setRecordInternal(Object scriptRecord, Record record) {
    try {
      MAP_PUT_METHOD.invoke(scriptRecord, "_record", record, MAP_PUT_MODE_READONLY);
    } catch (Exception ex) {
      throw new RuntimeException(REFLECTION_ERROR_MESSAGE + ex, ex);
    }
  }


  @Override
  protected Record getRecordInternal(Object scriptRecord) {
    return (Record) ((Map)scriptRecord).get("_record");
  }

  @Override
  public void putInMap(Object obj, Object key, Object value) {
    try {
      MAP_PUT_METHOD.invoke(obj, key, value, MAP_PUT_MODE_PERMANENT);
    } catch (Exception ex) {
      throw new RuntimeException(REFLECTION_ERROR_MESSAGE + ex, ex);
    }
  }

}
