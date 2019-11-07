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

package com.streamsets.pipeline.stage.scripting.testutils;

import com.streamsets.pipeline.api.base.BaseStage;
import org.junit.Assert;
import org.mockito.Mockito;
import org.python.jsr223.PyScriptEngine;

import javax.script.ScriptEngine;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class JythonStageTestUtil {

  public static ScriptEngine spyScriptEngine(ScriptEngine engine) {
    Assert.assertNotNull(engine);
    final PyScriptEngine pyScriptEngine = (PyScriptEngine) engine;
    return Mockito.spy(pyScriptEngine);
  }

  public static void closeAndAssertDestroyCalled(BaseStage<?> stage, ScriptEngine engine) {
    // ensure the engine is actually a PyScriptEngine, since that's the only kind that implements close
    assertThat(engine, instanceOf(PyScriptEngine.class));

    final PyScriptEngine pyScriptEngine = (PyScriptEngine) engine;

    // call destroy on the stage; this should call close on the underlying engine
    stage.destroy();

    // verify close was called
    Mockito.verify(pyScriptEngine).close();
  }
}
