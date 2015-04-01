/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.jython;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.AbstractScriptingProcessor;
import com.streamsets.pipeline.stage.processor.scripting.ScriptObjectFactory;
import org.python.core.PyDictionary;
import org.python.core.PyList;

import javax.script.ScriptEngine;
import javax.script.SimpleBindings;
import java.util.List;
import java.util.Map;

public class JythonProcessor extends AbstractScriptingProcessor {

  public static final String JYTHON_ENGINE = "jython";

  public JythonProcessor(ProcessingMode processingMode, String script) {
    super(JYTHON_ENGINE, Groups.JYTHON.name(), "script", processingMode, script);
  }

  @Override
  protected ScriptObjectFactory getScriptObjectFactory() {
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
    protected Record getRecordInternal(Object scriptRecord) {
      return (Record) ((PyDictionary) scriptRecord).get("_record");
    }

    @Override
    protected void setRecordInternal(Object scriptRecord, Record record) {
      putInMap(scriptRecord, "_record", record);
    }
  }
}
