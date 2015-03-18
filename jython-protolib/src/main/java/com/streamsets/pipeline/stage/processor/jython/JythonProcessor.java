/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.jython;

import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ScriptingProcessor;

public class JythonProcessor extends ScriptingProcessor {
  private static final String IMPORT_FIELD_API = "from com.streamsets.pipeline.api import Field\n";

  public static final String JYTHON_ENGINE = "jython";

  public JythonProcessor(ProcessingMode processingMode, String script) {
    super(JYTHON_ENGINE, Groups.JYTHON.name(), "script", processingMode, script);
  }

  @Override
  protected String prepareScript(String script) {
    return IMPORT_FIELD_API + super.prepareScript(script);
  }

}
