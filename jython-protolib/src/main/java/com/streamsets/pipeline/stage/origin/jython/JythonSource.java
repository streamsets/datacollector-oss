/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.jython;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.origin.scripting.AbstractScriptingSource;
import com.streamsets.pipeline.stage.origin.scripting.config.ScriptSourceConfigBean;
import com.streamsets.pipeline.stage.util.scripting.ScriptObjectFactory;
import com.streamsets.pipeline.stage.util.scripting.ScriptTypedNullObject;
import com.streamsets.pipeline.stage.util.scripting.config.ScriptRecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;


public class JythonSource extends AbstractScriptingSource {

  private static final Logger LOG = LoggerFactory.getLogger(JythonSource.class);
  private static final String JYTHON_ENGINE = "jython";
  private final ScriptRecordType scriptRecordType;

  public JythonSource(String script, ScriptSourceConfigBean scriptConf) {
    super(
        LOG,
        JYTHON_ENGINE,
        "Jython",
        script,
        scriptConf
    );
    this.scriptRecordType = scriptConf.scriptRecordType;
  }

  @Override
  protected ScriptObjectFactory createScriptObjectFactory(Stage.Context context) {
    return new JythonScriptObjectFactory(engine, context, scriptRecordType);
  }

  private static class JythonScriptObjectFactory extends ScriptObjectFactory {

    public JythonScriptObjectFactory(ScriptEngine scriptEngine, Stage.Context context, ScriptRecordType scriptRecordType) {
      super(scriptEngine, context, scriptRecordType);
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
        field = Field.createDatetime((Date) scriptObject);
      } else if (scriptObject instanceof BigDecimal) {
        field = Field.create((BigDecimal) scriptObject);
      } else if (scriptObject instanceof String) {
        field = Field.create((String) scriptObject);
      } else if (scriptObject instanceof byte[]) {
        field = Field.create((byte[]) scriptObject);
      } else if (scriptObject instanceof ScriptObjectFactory.ScriptFileRef) {
        field = Field.create(getFileRefFromScriptFileRef((ScriptObjectFactory.ScriptFileRef) scriptObject));
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

}
