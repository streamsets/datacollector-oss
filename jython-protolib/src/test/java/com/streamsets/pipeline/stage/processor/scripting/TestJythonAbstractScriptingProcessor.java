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

package com.streamsets.pipeline.stage.processor.scripting;

import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.processor.jython.JythonProcessor;
import com.streamsets.pipeline.stage.scripting.testutils.JythonStageTestUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * This is deliberately kept in the same package as {@link AbstractScriptingProcessor} because it needs to access
 * the engine field, which is package protected.
 */
public class TestJythonAbstractScriptingProcessor {

  @Test
  public void testCloseCalledOnStageDestroy() {
    final JythonProcessor processor = new JythonProcessor(
        ProcessingMode.RECORD,
        "dummy script (won't be compiled)"
    );

    // initialize the processor; this creates the engine
    processor.init(Mockito.mock(Stage.Info.class), Mockito.mock(Processor.Context.class));

    // swap out the engine with a spied version
    processor.engine = JythonStageTestUtil.spyScriptEngine(processor.engine);

    // destroy the processor and ensure close is called (via the spy)
    JythonStageTestUtil.closeAndAssertDestroyCalled(processor, processor.engine);
    Assert.assertNull(processor.engine);
  }
}
