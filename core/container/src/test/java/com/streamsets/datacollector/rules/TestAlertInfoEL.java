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
package com.streamsets.datacollector.rules;

import com.streamsets.datacollector.definition.ConcreteELDefinitionExtractor;
import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import org.junit.Assert;
import org.junit.Test;

public class TestAlertInfoEL {

  @ElFunction(name = "setInfo")
  public static String setInfo() {
    AlertInfoEL.setInfo("Hello");
    return "";
  }

  @Test
  public void testAlertInfo() throws Exception {
    ELVars vars = new ELVariables();

    // setting the alert:info via a dummy EL 'setInfo' which has an ELVars in context
    ELEval elEval = new ELEvaluator("", ConcreteELDefinitionExtractor.get(), TestAlertInfoEL.class);
    elEval.eval(vars, "${setInfo()}", String.class);

    elEval = new ELEvaluator("", ConcreteELDefinitionExtractor.get(), AlertInfoEL.class);
    Assert.assertEquals("Hello", elEval.eval(vars, "${alert:info()}", String.class));
  }

}
