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
package com.streamsets.datacollector.el;

import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.pipeline.api.el.ELVars;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestELVariables {

  private static final String VAR_NAME = "name";
  private static final Integer VAR_VALUE = 5;

  @Test
  public void testELVariables() {
    Map<String, Object> constants = new HashMap<>();
    constants.put("X", "x");
    ELVars elVars = new ELVariables(constants);

    Assert.assertEquals("x", elVars.getConstant("X"));

    elVars.addContextVariable(VAR_NAME, VAR_VALUE);
    Assert.assertTrue(elVars.hasContextVariable(VAR_NAME));
    Assert.assertFalse(elVars.hasVariable(VAR_NAME));
    Assert.assertEquals(VAR_VALUE, elVars.getContextVariable(VAR_NAME));
    Assert.assertEquals(null, elVars.getVariable(VAR_NAME));

    elVars = new ELVariables();

    elVars.addVariable(VAR_NAME, VAR_VALUE);
    Assert.assertFalse(elVars.hasContextVariable(VAR_NAME));
    Assert.assertTrue(elVars.hasVariable(VAR_NAME));
    Assert.assertEquals(VAR_VALUE, elVars.getVariable(VAR_NAME));
    Assert.assertEquals(null, elVars.getContextVariable(VAR_NAME));

  }

  @Test
  public void testInvalidVariableName1() {
    ELVars elVars = new ELVariables();
    try {
      elVars.addContextVariable(null, VAR_VALUE);
      Assert.fail("NullPointerException expected.");
    } catch (NullPointerException e) {

    }

    try {
      elVars.addVariable(null, VAR_VALUE);
      Assert.fail("NullPointerException expected.");
    } catch (NullPointerException e) {

    }

    try {
      elVars.addContextVariable("$hello", VAR_VALUE);
      Assert.fail("IllegalArgumentException expected.");
    } catch (IllegalArgumentException e) {

    }

    try {
      elVars.addVariable("$hello", VAR_VALUE);
      Assert.fail("IllegalArgumentException expected.");
    } catch (IllegalArgumentException e) {

    }
  }

  @Test
  public void testReplaceConstant() {
    Map<String, Object> constants = new HashMap<>();
    constants.put("ZERO", 0);

    ELVars elVars = new ELVariables(constants);
    elVars.addVariable("ZERO", 1);
    Assert.assertEquals(0, elVars.getConstant("ZERO"));
    Assert.assertEquals(1, elVars.getVariable("ZERO"));
  }

}
