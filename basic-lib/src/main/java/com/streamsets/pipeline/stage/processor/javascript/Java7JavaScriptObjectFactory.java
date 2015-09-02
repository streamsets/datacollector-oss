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
package com.streamsets.pipeline.stage.processor.javascript;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.stage.processor.scripting.ScriptObjectFactory;

import javax.script.ScriptEngine;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;

public class Java7JavaScriptObjectFactory extends ScriptObjectFactory {
  private static final Class MAP_OBJECT_CLASS;
  private static final Method MAP_PUT_METHOD;
  private static final int MAP_PUT_MODE_PERMANENT;
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
      return ARRAY_CONSTRUCTOR.newInstance(new Object[]{elements.toArray(new Object[elements.size()])});
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
  public void putInMap(Object obj, Object key, Object value) {
    try {
      MAP_PUT_METHOD.invoke(obj, key, value, MAP_PUT_MODE_PERMANENT);
    } catch (Exception ex) {
      throw new RuntimeException(REFLECTION_ERROR_MESSAGE + ex, ex);
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
