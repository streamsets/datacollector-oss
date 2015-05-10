package com.streamsets.pipeline.stage.processor.javascript;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.stage.processor.scripting.ScriptObjectFactory;
import jdk.nashorn.api.scripting.ScriptObjectMirror;

import javax.script.ScriptEngine;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Java8JavaScriptObjectFactory extends ScriptObjectFactory {
  public Java8JavaScriptObjectFactory(ScriptEngine engine) {
    super(engine);
  }

  @Override
  protected Field scriptToField(Object scriptObject) {
    Field field;
    if (scriptObject != null) {
      if (scriptObject instanceof ScriptObjectMirror) {
        ScriptObjectMirror scriptMap = (ScriptObjectMirror) scriptObject;
        if (scriptMap.isArray()) {
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