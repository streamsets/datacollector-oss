/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.javascript;

import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ScriptingProcessor;

public class JavaScriptProcessor extends ScriptingProcessor {
  private static final String IMPORT_FIELD_API = "var Field = com.streamsets.pipeline.api.Field\n";

  public static final String JAVASCRIPT_ENGINE = "rhino";

  public JavaScriptProcessor(ProcessingMode processingMode, String script) {
    super(JAVASCRIPT_ENGINE, Groups.JAVASCRIPT.name(), "script", processingMode, script);
  }

  @Override
  protected String prepareScript(String script) {
    return IMPORT_FIELD_API + super.prepareScript(script);
  }

}
