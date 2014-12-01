/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.el;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

public class TestELEvaluator {

  public static int idFunction(int i) {
    return i;
  }

  public static Object varViaContextFunction(String varName) {
    return ELEvaluator.getVariablesInScope().getVariable(varName);
  }

  public static Object contextVarFunction(String varName) {
    return ELEvaluator.getVariablesInScope().getContextVariable(varName);
  }

  @Test
  public void testELEvaluator() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertTrue(eval.eval(variables, "${1 eq 1}", Boolean.class));
    Assert.assertEquals(Boolean.TRUE, eval.eval(variables, "${1 eq 1}"));

    variables.addVariable("a", "A");
    Assert.assertTrue(variables.hasVariable("a"));

    Assert.assertEquals(Boolean.TRUE, eval.eval(variables, "${a eq 'A'}"));

    Method function = getClass().getMethod("idFunction", Integer.TYPE);

    eval.registerFunction("", "id", function);
    Assert.assertEquals(Boolean.TRUE, eval.eval(variables, "${id(1) eq 1}"));

    eval.registerFunction("id", "id", function);
    Assert.assertEquals(Boolean.TRUE, eval.eval(variables, "${id:id(1) eq 1}"));

    function = getClass().getMethod("varViaContextFunction", String.class);

    eval.registerFunction("", "varViaContext", function);
    Assert.assertEquals(Boolean.TRUE, eval.eval(variables, "${varViaContext('a') eq 'A'}"));

    eval.registerConstant("X", "x");
    Assert.assertEquals("x", eval.eval(variables, "${X}"));

    function = getClass().getMethod("contextVarFunction", String.class);

    variables.addContextVariable("c", "C");
    Assert.assertTrue(variables.hasContextVariable("c"));

    eval.registerFunction("", "contextVar", function);
    Assert.assertEquals(Boolean.TRUE, eval.eval(variables, "${contextVar('c') eq 'C'}"));

  }

}
