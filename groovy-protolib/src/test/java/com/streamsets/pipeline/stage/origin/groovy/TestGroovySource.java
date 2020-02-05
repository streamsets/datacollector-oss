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

import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.stage.origin.scripting.AbstractScriptingDSource;
import com.streamsets.pipeline.stage.origin.scripting.ScriptingOriginTestUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestGroovySource {

  private static final Class DSOURCECLASS = GroovyDSource.class;

  private static AbstractScriptingDSource getDSource() {
    return new GroovyDSource();
  }

  @Test
  public void testResumeGenerateRecords() throws Exception {
    ScriptingOriginTestUtil.testResumeGenerateRecords(
        DSOURCECLASS,
        getDSource(),
        "GeneratorOriginScript.groovy"
    );
  }

  @Test
  public void testAllBindings() throws Exception {
    ScriptingOriginTestUtil.testAllBindings(
        DSOURCECLASS,
        getDSource(),
        "TestAllBindings.groovy"
    );
  }

  @Test
  public void testNullTypes() throws Exception {
    ScriptingOriginTestUtil.testNullTypes(
        DSOURCECLASS,
        getDSource(),
        "TestNullTypes.groovy"
    );
  }

  @Test
  public void testUserCodeInjectionFlag() {
    Assert.assertArrayEquals(
        "This stage should _only_ have the USER_CODE_INJECTION flag set",
        new StageBehaviorFlags[]{StageBehaviorFlags.USER_CODE_INJECTION},
        GroovyDSource.class.getAnnotation(StageDef.class).flags()
    );
  }
}
