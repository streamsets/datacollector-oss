/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import org.junit.Assert;
import org.junit.Test;

public class TestELBasicSupport {

  @Test
  public void testRecordFunctions() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELBasicSupport.registerBasicFunctions(eval);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertTrue(eval.eval(variables, "${isIn('a', 'a,b,c,d')}", Boolean.class));
    Assert.assertTrue(eval.eval(variables, "${notIn('e', 'a,b,c,d')}", Boolean.class));
  }

}
