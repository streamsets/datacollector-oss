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

package com.streamsets.pipeline.stage.origin.javascript;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.origin.scripting.AbstractScriptingSource;
import com.streamsets.pipeline.stage.origin.scripting.config.ScriptSourceConfigBean;
import com.streamsets.pipeline.stage.util.scripting.ScriptObjectFactory;
import com.streamsets.pipeline.stage.util.scripting.config.ScriptRecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;

public class JavascriptSource extends AbstractScriptingSource {

  private static final Logger LOG = LoggerFactory.getLogger(JavascriptSource.class);
  private final ScriptRecordType scriptRecordType;

  public static final String JAVASCRIPT_ENGINE = "javascript";

  public JavascriptSource(String script, ScriptSourceConfigBean scriptConf) {
    super(
        LOG,
        JAVASCRIPT_ENGINE,
        "Javascript",
        script,
        scriptConf
    );
    this.scriptRecordType = scriptConf.scriptRecordType;
  }

  @Override
  protected ScriptObjectFactory createScriptObjectFactory(Stage.Context context) {
    return new JavascriptObjectFactory(engine, context, scriptRecordType);
  }

  private static class JavascriptObjectFactory extends ScriptObjectFactory {

    public JavascriptObjectFactory(ScriptEngine scriptEngine, Stage.Context context, ScriptRecordType scriptRecordType) {
      super(scriptEngine, context, scriptRecordType);
    }

  }

}