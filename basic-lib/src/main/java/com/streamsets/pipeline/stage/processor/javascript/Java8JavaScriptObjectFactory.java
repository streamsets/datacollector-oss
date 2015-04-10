package com.streamsets.pipeline.stage.processor.javascript;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.stage.processor.scripting.ScriptObjectFactory;

import javax.script.ScriptEngine;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Java8JavaScriptObjectFactory extends ScriptObjectFactory {
  private static final Class NASHHORN_SCRIPT_ENGINE_CLASS;
  private static final Class CONTEXT_CLASS;
  private static final Class GLOBAL_CLASS;
  private static final Class SCRIPT_OBJECT_CLASS;
  private static final Class NATIVE_ARRAY_CLASS;
  private static final java.lang.reflect.Field NASHHORN_SCRIPT_ENGINE_CONTEXT_FIELD;
  private static final Method CONTEXT_NEW_GLOBAL_METHOD;
  private static final Method CONTEXT_SET_GLOBAL_METHOD;
  private static final Method GLOBAL_NEW_OBJECT_METHOD;
  private static final Method SCRIPT_OBJECT_ENTRYSET_METHOD;
  private static final Method SCRIPT_OBJECT_PUT_METHOD;
  private static final Method NATIVE_ARRAY_AS_OBJECT_ARRAY_METHOD;
  private static final Constructor ARRAY_CONSTRUCTOR;
  private static final String REFLECTION_ERROR_MESSAGE = "Error performing reflection on " +
    System.getProperty("java.version") + ". Please report: ";
  private final Object context;
  private final Object global;

  // we are doing this reflection voodoo because the Java compile does not let you use sun....internal packages
  // PRRRRRRRRHHHHHHHHHHH Oracle.
  static {
    try {
      //
      NASHHORN_SCRIPT_ENGINE_CLASS = ClassLoader.getSystemClassLoader().loadClass(
        "jdk.nashorn.api.scripting.NashornScriptEngine");
      CONTEXT_CLASS = ClassLoader.getSystemClassLoader().loadClass(
        "jdk.nashorn.internal.runtime.Context");
      SCRIPT_OBJECT_CLASS = ClassLoader.getSystemClassLoader().loadClass(
        "jdk.nashorn.internal.runtime.ScriptObject");
      NATIVE_ARRAY_CLASS = ClassLoader.getSystemClassLoader().loadClass(
        "jdk.nashorn.internal.objects.NativeArray");
      GLOBAL_CLASS = ClassLoader.getSystemClassLoader().loadClass(
        "jdk.nashorn.internal.objects.Global");
      NASHHORN_SCRIPT_ENGINE_CONTEXT_FIELD = NASHHORN_SCRIPT_ENGINE_CLASS.getDeclaredField("nashornContext");
      NASHHORN_SCRIPT_ENGINE_CONTEXT_FIELD.setAccessible(true);
      CONTEXT_SET_GLOBAL_METHOD = CONTEXT_CLASS.getMethod("setGlobal", SCRIPT_OBJECT_CLASS);
      CONTEXT_NEW_GLOBAL_METHOD = CONTEXT_CLASS.getMethod("newGlobal");
      GLOBAL_NEW_OBJECT_METHOD = GLOBAL_CLASS.getMethod("newObject");
      SCRIPT_OBJECT_ENTRYSET_METHOD = SCRIPT_OBJECT_CLASS.getMethod("entrySet");
      SCRIPT_OBJECT_PUT_METHOD = SCRIPT_OBJECT_CLASS.getMethod("put", Object.class, Object.class, Boolean.TYPE);
      NATIVE_ARRAY_AS_OBJECT_ARRAY_METHOD = NATIVE_ARRAY_CLASS.getMethod("asObjectArray");
      ARRAY_CONSTRUCTOR = NATIVE_ARRAY_CLASS.getDeclaredConstructor(Object[].class);
      ARRAY_CONSTRUCTOR.setAccessible(true);
    } catch (Exception ex) {
      throw new RuntimeException(REFLECTION_ERROR_MESSAGE + ex, ex);
    }
  }
  public Java8JavaScriptObjectFactory(ScriptEngine engine) {
    super(engine);
    try {
      context = NASHHORN_SCRIPT_ENGINE_CONTEXT_FIELD.get(engine);
      global = CONTEXT_NEW_GLOBAL_METHOD.invoke(context);
      CONTEXT_SET_GLOBAL_METHOD.invoke(null, global);
    } catch(Exception ex) {
      throw new RuntimeException(REFLECTION_ERROR_MESSAGE + ex, ex);
    }
  }
  @Override
  public Object createArray(List elements) {
    try {
      return ARRAY_CONSTRUCTOR.newInstance(new Object[]{elements.toArray(new Object[elements.size()])});
    } catch (Exception ex) {
      throw new RuntimeException(REFLECTION_ERROR_MESSAGE + ex, ex);
    }
  }

  @Override
  public Object createMap() {
    try {
      return GLOBAL_NEW_OBJECT_METHOD.invoke(global);
    } catch (Exception ex) {
      throw new RuntimeException(REFLECTION_ERROR_MESSAGE + ex, ex);
    }
  }

  @Override
  public void putInMap(Object obj, Object key, Object value) {
    try {
      SCRIPT_OBJECT_PUT_METHOD.invoke(obj, key, value, true);
    } catch (Exception ex) {
      throw new RuntimeException(REFLECTION_ERROR_MESSAGE + ex, ex);
    }
  }

  @Override
  protected Field scriptToField(Object scriptObject) {
    Field field;
    if (scriptObject != null) {
      if (NATIVE_ARRAY_CLASS.isInstance(scriptObject)) {
        try {
          Object[] values = (Object[]) NATIVE_ARRAY_AS_OBJECT_ARRAY_METHOD.invoke(scriptObject);
          List<Field> fieldArray = new ArrayList<>(values.length);
          for (Object element : values) {
            fieldArray.add(scriptToField(element));
          }
          field = Field.create(fieldArray);
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      } else if (SCRIPT_OBJECT_CLASS.isInstance(scriptObject)) {
        Map<Object, Object> scriptMap = new ScriptObjectMap(scriptObject);
        Map<String, Field> fieldMap = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> entry : scriptMap.entrySet()) {
          fieldMap.put(entry.getKey().toString(), scriptToField(entry.getValue()));
        }
        field = Field.create(fieldMap);
      } else {
        field = convertPrimitiveObject(scriptObject);
      }
    } else {
      field = Field.create((String)null);
    }
    return field;
  }

  private static class ScriptObjectMap extends AbstractMap<Object, Object> {
    private final Object scriptMap;
    private ScriptObjectMap(Object scriptMap) {
      this.scriptMap = scriptMap;
    }
    @Override
    public Set<Entry<java.lang.Object, java.lang.Object>> entrySet() {
      try {
        return (Set<Entry<Object, Object>>)SCRIPT_OBJECT_ENTRYSET_METHOD.invoke(scriptMap);
      } catch (Exception ex) {
        throw new RuntimeException(REFLECTION_ERROR_MESSAGE + ex, ex);
      }
    }
  }

  @Override
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
