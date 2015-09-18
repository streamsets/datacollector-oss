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
package com.streamsets.pipeline.stage.processor.jython;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.stage.processor.scripting.AbstractScriptingProcessor;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ScriptObjectFactory;
import org.python.core.PyDictionary;
import org.python.core.PyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;

public class JythonProcessor extends AbstractScriptingProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(JythonProcessor.class);

  public static final String JYTHON_ENGINE = "jython";

  public JythonProcessor(ProcessingMode processingMode, String script) {
    super(LOG, JYTHON_ENGINE, Groups.JYTHON.name(), "script", processingMode, script);
  }

  @Override
  protected ScriptObjectFactory createScriptObjectFactory() {
    return new JythonScriptObjectFactory(engine);
  }

  private class JythonScriptObjectFactory extends ScriptObjectFactory {

    public JythonScriptObjectFactory(ScriptEngine scriptEngine) {
      super(scriptEngine);
    }

    @Override
    public void putInMap(Object obj, Object key, Object value) {
      ((PyDictionary) obj).put(key, value);
    }

    @Override
    public Object createMap() {
      return new PyDictionary();
    }

    @Override
    public Object createArray(List elements) {
      PyList list = new PyList();
      for (Object element : elements) {
        list.add(element);
      }
      return list;
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
      } else if (scriptObject instanceof BigInteger) { // special handling for Jython LONG type
        field = Field.create(((BigInteger) scriptObject).longValue());
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

}
