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
package com.streamsets.pipeline.stage.processor.javascript;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.processor.scripting.AbstractScriptingProcessor;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.util.scripting.ScriptObjectFactory;
import com.streamsets.pipeline.stage.util.scripting.config.ScriptRecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class JavaScriptProcessor extends AbstractScriptingProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(JavaScriptProcessor.class);

  public static final String JAVASCRIPT_ENGINE = "javascript";
  private final ScriptRecordType scriptRecordType;

  public JavaScriptProcessor(ProcessingMode processingMode, String script, String initScript, String destroyScript,
                             ScriptRecordType scriptRecordType, Map<String, String> userParams) {
    super(LOG, JAVASCRIPT_ENGINE, Groups.JAVASCRIPT.name(), processingMode, script, initScript, destroyScript,
        userParams);
    this.scriptRecordType = scriptRecordType;
  }

  // For tests
  public JavaScriptProcessor(ProcessingMode processingMode, String script) {
    this(processingMode, script, "", "", ScriptRecordType.NATIVE_OBJECTS, new HashMap<>());
  }

  @Override
  protected ScriptObjectFactory createScriptObjectFactory(Stage.Context context) {
    return ScriptObjectFactoryFactory.getScriptObjectFactory(engine, context, scriptRecordType);
  }

}
