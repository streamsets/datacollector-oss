/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.el;

import org.junit.Assert;
import org.junit.Test;

import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.el.JvmEL;
import com.streamsets.datacollector.el.RuleELRegistry;

public class TestJvmEL {

  @Test
  public void testMaxMemory() throws Exception {
    ELEvaluator eval = new ELEvaluator("x", JvmEL.class);
    ELVariables variables = new ELVariables();
    Assert.assertTrue(eval.eval(variables, "${jvm:maxMemoryMB()}", Long.class) > 0);
  }

  @Test
  public void testJvmELAvailViaRuleELRegistry() throws Exception {
    ELEvaluator eval = new ELEvaluator("x", RuleELRegistry.getRuleELs());
    ELVariables variables = new ELVariables();
    Assert.assertTrue(eval.eval(variables, "${jvm:maxMemoryMB()}", Long.class) > 0);
  }

}
