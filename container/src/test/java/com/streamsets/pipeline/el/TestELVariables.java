/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

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
    ELVars elVars = new ELVariables();

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
  public void testInvalidVariableName2() {
    Map<String, Object> constants = new HashMap<>();
    constants.put("ZERO", 0);

    ELVars elVars = new ELVariables(constants);

    try {
      elVars.addContextVariable("ZERO", VAR_VALUE);
      Assert.fail("IllegalArgumentException expected as a constant with the same name already exists.");
    } catch (IllegalArgumentException e) {

    }

    try {
      elVars.addVariable("ZERO", VAR_VALUE);
      Assert.fail("IllegalArgumentException expected as a constant with the same name already exists.");
    } catch (IllegalArgumentException e) {

    }
  }
}
