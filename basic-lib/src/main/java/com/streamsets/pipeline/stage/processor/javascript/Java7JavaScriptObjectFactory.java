/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.javascript;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.processor.scripting.ScriptObjectFactory;

import javax.script.ScriptEngine;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import com.streamsets.pipeline.stage.processor.scripting.ScriptTypedNullObject;
import sun.org.mozilla.javascript.internal.NativeObject;
import sun.org.mozilla.javascript.internal.NativeArray;

public class Java7JavaScriptObjectFactory extends ScriptObjectFactory {

  public Java7JavaScriptObjectFactory(ScriptEngine engine, Stage.Context context) {
    super(engine, context);
  }

  @Override
  public Object createArray(List elements) {
    return new NativeArray(elements.toArray(new Object[elements.size()]));
  }

  public static class NativeObjectMapInfo extends NativeObject implements MapInfo {
    private final boolean isListMap;

    public NativeObjectMapInfo(boolean isListMap) {
      this.isListMap = isListMap;
    }

    @Override
    public boolean isListMap() {
      return isListMap;
    }
  }

  @Override
  public Object createMap(boolean isListMap) {
    return new NativeObjectMapInfo(isListMap);
  }

  @Override
  public void putInMap(Object obj, Object key, Object value) {
    ((NativeObject)obj).defineProperty((String)key, value, NativeObject.PERMANENT);
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
      field = Field.createDatetime((Date) scriptObject);
    } else if (scriptObject instanceof BigDecimal) {
      field = Field.create((BigDecimal) scriptObject);
    } else if (scriptObject instanceof String) {
      field = Field.create((String) scriptObject);
    } else if (scriptObject instanceof byte[]) {
      field = Field.create((byte[]) scriptObject);
    } else {
      field = ScriptTypedNullObject.getTypedNullFieldFromScript(scriptObject);
      if (field == null) {
        // unable to find field type from scriptObject. Return null String.
        field = Field.create(scriptObject.toString());
      }
    }
    return field;
  }

}
