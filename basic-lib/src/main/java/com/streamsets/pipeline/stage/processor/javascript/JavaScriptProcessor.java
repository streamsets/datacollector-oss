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

import java.util.List;
import java.util.Map;

public class JavaScriptProcessor extends AbstractScriptingProcessor {

  public static final String JAVASCRIPT_ENGINE = "javascript";

  public JavaScriptProcessor(ProcessingMode processingMode, String script) {
    super(JAVASCRIPT_ENGINE, Groups.JAVASCRIPT.name(), "script", processingMode, script);
  }

  protected ScriptObjectFactory getScriptObjectFactory() {
    return ScriptObjectFactoryFactory.getScriptObjectFactory(engine);
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
