package com.streamsets.pipeline.stage.processor.javascript;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.stage.processor.scripting.ScriptObjectFactory;
import jdk.nashorn.api.scripting.ScriptObjectMirror;

import javax.script.ScriptEngine;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Java8JavaScriptObjectFactory extends ScriptObjectFactory {
  public Java8JavaScriptObjectFactory(ScriptEngine engine) {
    super(engine);
  }

  @Override
  public Object createArray(List elements) {
    // Nashorn can pass Java objects to JavaScript directly.
    return elements;
  }

  @Override
  public Object createMap() {
    // Nashorn can pass Java objects to JavaScript directly.
    return new HashMap();
  }

  @Override
  public void putInMap(Object obj, Object key, Object value) {
      ((ScriptObjectMirror) obj).put(key.toString(), value);
  }

  @Override
  protected Field scriptToField(Object scriptObject) {
    Field field;
    if (scriptObject != null) {
      if (scriptObject instanceof ScriptObjectMirror) {
        ScriptObjectMirror scriptMap = (ScriptObjectMirror) scriptObject;

        boolean isArray = false;
        if (((ScriptObjectMirror) scriptObject).isArray()) {
          isArray = true;
        }

        if (isArray) {
          List<Field> fields = new ArrayList<>(scriptMap.entrySet().size());
          for (Map.Entry<String, Object> entry : scriptMap.entrySet()) {
            fields.add(scriptToField(entry.getValue()));
          }
          field = Field.create(fields);
        } else {
          Map<String, Field> fields = new LinkedHashMap<>();
          for (Map.Entry<String, Object> entry : scriptMap.entrySet()) {
            fields.put(entry.getKey(), scriptToField(entry.getValue()));
          }
          field = Field.create(fields);
        }
      } else {
        field = convertPrimitiveObject(scriptObject);
      }
    } else {
      field = Field.create((String)null);
    }
    return field;
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
