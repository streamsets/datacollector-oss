/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.groovy;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.processor.scripting.AbstractScriptingProcessor;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ScriptObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;

public class GroovyProcessor extends AbstractScriptingProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(GroovyProcessor.class);

  static final String GROOVY_ENGINE = "groovy-sdc";
  static final String GROOVY_INDY_ENGINE = "groovy-sdc-indy";

  public GroovyProcessor(
      ProcessingMode processingMode,
      String script,
      String initScript,
      String destroyScript,
      String engineName
  ) {
    super(LOG, engineName, Groups.GROOVY.name(), processingMode, script, initScript, destroyScript);
  }

  public GroovyProcessor(ProcessingMode processingMode, String script, String engineName) {
    this(processingMode, script, "", "", engineName);
  }

  public GroovyProcessor(ProcessingMode processingMode, String script, String initScript, String destroyScript) {
    this(processingMode, script, initScript, destroyScript, GROOVY_ENGINE);
  }

  public GroovyProcessor(ProcessingMode processingMode, String script) {
    this(processingMode, script, GROOVY_ENGINE);
  }

  @Override
  protected ScriptObjectFactory createScriptObjectFactory(Stage.Context context) {
    return new GroovyScriptObjectFactory(engine, context);
  }

  private static class GroovyScriptObjectFactory extends ScriptObjectFactory {
    public GroovyScriptObjectFactory(ScriptEngine engine, Stage.Context context) {
      super(engine, context);
    }
  }
}
