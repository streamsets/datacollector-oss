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

package com.streamsets.pipeline.stage.origin.scripting;

import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.origin.jython.JythonSource;
import com.streamsets.pipeline.stage.origin.scripting.config.ScriptSourceConfigBean;
import com.streamsets.pipeline.stage.scripting.testutils.JythonStageTestUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * This is deliberately kept in the same package as {@link AbstractScriptingSource} because it needs to access
 * the engine field, which is package protected.
 */
public class TestJythonAbstractScriptingSource {

  @Test
  public void testCloseCalledOnStageDestroy() {
    final JythonSource source = new JythonSource(
        "dummy script (won't be compiled)",
        new ScriptSourceConfigBean()
    );

    // initialize the source; this creates the engine
    source.init(Mockito.mock(Stage.Info.class), Mockito.mock(PushSource.Context.class));

    // swap out the engine with a spied version
    source.engine = JythonStageTestUtil.spyScriptEngine(source.engine);

    // destroy the source and ensure close is called (via the spy)
    JythonStageTestUtil.closeAndAssertDestroyCalled(source, source.engine);
    Assert.assertNull(source.engine);
  }
}
