/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.javascript;

import com.streamsets.pipeline.stage.processor.scripting.AbstractScriptingProcessor;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ScriptObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaScriptProcessor extends AbstractScriptingProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(JavaScriptProcessor.class);

  public static final String JAVASCRIPT_ENGINE = "javascript";

  public JavaScriptProcessor(ProcessingMode processingMode, String script) {
    super(LOG, JAVASCRIPT_ENGINE, Groups.JAVASCRIPT.name(), "script", processingMode, script);
  }

  protected ScriptObjectFactory createScriptObjectFactory() {
    return ScriptObjectFactoryFactory.getScriptObjectFactory(engine);
  }

}
