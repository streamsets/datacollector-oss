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

package com.streamsets.pipeline.stage.origin.groovy;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.origin.scripting.AbstractScriptingSource;
import com.streamsets.pipeline.stage.origin.scripting.config.ScriptSourceConfigBean;
import com.streamsets.pipeline.stage.util.scripting.ScriptObjectFactory;
import com.streamsets.pipeline.stage.util.scripting.config.ScriptRecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;

public class GroovySource extends AbstractScriptingSource {

  private static final Logger LOG = LoggerFactory.getLogger(GroovySource.class);
  private final ScriptRecordType scriptRecordType;

  static final String GROOVY_ENGINE = "groovy-sdc";
  static final String GROOVY_INDY_ENGINE = "groovy-sdc-indy";

  public GroovySource(String script, ScriptSourceConfigBean scriptConf) {
    super(
        LOG,
        GROOVY_ENGINE,
        "Groovy",
        script,
        scriptConf
    );
    this.scriptRecordType = scriptConf.scriptRecordType;
  }

  @Override
  protected ScriptObjectFactory createScriptObjectFactory(Stage.Context context) {
    return new GroovyScriptObjectFactory(engine, context, scriptRecordType);
  }

  private static class GroovyScriptObjectFactory extends ScriptObjectFactory {

    public GroovyScriptObjectFactory(ScriptEngine scriptEngine, Stage.Context context, ScriptRecordType scriptRecordType) {
      super(scriptEngine, context, scriptRecordType);
    }

  }

}
