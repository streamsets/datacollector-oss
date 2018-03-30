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

import com.streamsets.datacollector.definition.ConcreteELDefinitionExtractor;
import com.streamsets.pipeline.lib.el.MathEL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class TestMathEL {

  ELEvaluator eval;
  ELVariables variables;

  @Before
  public void setUpELs() {
    eval = new ELEvaluator("test", ConcreteELDefinitionExtractor.get(), MathEL.class);
    variables = new ELVariables();
  }

  @Test
  public void testStringCasting() throws Exception {
    Assert.assertEquals(1.0, eval.eval(variables, "${math:abs(\"1.0\")}", Double.class), 0.1);
    Assert.assertEquals(1.0, eval.eval(variables, "${math:abs(\"-1\")}", Double.class), 0.1);


    Assert.assertEquals(1.0, eval.eval(variables, "${math:max(1,     \"-1\")}", Double.class), 0.1);
    Assert.assertEquals(1.0, eval.eval(variables, "${math:max(\"-1.0\", 1.0)}", Double.class), 0.1);
  }

  @Test
  public void testAbs() throws Exception {
    Assert.assertEquals(1.0, eval.eval(variables, "${math:abs(1)}", Double.class), 0.1);
    Assert.assertEquals(1.0, eval.eval(variables, "${math:abs(-1)}", Double.class), 0.1);

    Assert.assertEquals(1.1, eval.eval(variables, "${math:abs(1.1)}", Double.class), 0.1);
    Assert.assertEquals(1.1, eval.eval(variables, "${math:abs(-1.1)}", Double.class), 0.1);
  }

  @Test
  public void testCeil() throws Exception {
    Assert.assertEquals(1.0, eval.eval(variables, "${math:ceil(0.999)}", Double.class), 0.1);
    Assert.assertEquals(1.0, eval.eval(variables, "${math:ceil(0.0009)}", Double.class), 0.1);

    Assert.assertEquals(0.0, eval.eval(variables, "${math:ceil(-0.999)}", Double.class), 0.1);
    Assert.assertEquals(0.0, eval.eval(variables, "${math:ceil(-0.0009)}", Double.class), 0.1);
  }

  @Test
  public void testFloor() throws Exception {
    Assert.assertEquals(0.0, eval.eval(variables, "${math:floor(0.999)}", Double.class), 0.1);
    Assert.assertEquals(0.0, eval.eval(variables, "${math:floor(0.0009)}", Double.class), 0.1);

    Assert.assertEquals(-1.0, eval.eval(variables, "${math:floor(-0.999)}", Double.class), 0.1);
    Assert.assertEquals(-1.0, eval.eval(variables, "${math:floor(-0.0009)}", Double.class), 0.1);
  }

  @Test
  public void testMax() throws Exception {
    Assert.assertEquals(1.0, eval.eval(variables, "${math:max(1, -1)}", Double.class), 0.1);
    Assert.assertEquals(1.0, eval.eval(variables, "${math:max(-1, 1)}", Double.class), 0.1);

    Assert.assertEquals(1.0, eval.eval(variables, "${math:max(1.0, -1.0)}", Double.class), 0.1);
    Assert.assertEquals(1.0, eval.eval(variables, "${math:max(-1.0, 1.0)}", Double.class), 0.1);
  }

  @Test
  public void testMin() throws Exception {
    Assert.assertEquals(-1.0, eval.eval(variables, "${math:min(1, -1)}", Double.class), 0.1);
    Assert.assertEquals(-1.0, eval.eval(variables, "${math:min(-1, 1)}", Double.class), 0.1);

    Assert.assertEquals(-1.0, eval.eval(variables, "${math:min(1.0, -1.0)}", Double.class), 0.1);
    Assert.assertEquals(-1.0, eval.eval(variables, "${math:min(-1.0, 1.0)}", Double.class), 0.1);
  }

  @Test
  public void testRound() throws Exception {
    Assert.assertEquals(1.0, eval.eval(variables, "${math:round(0.999)}", Double.class), 0.1);
    Assert.assertEquals(0.0, eval.eval(variables, "${math:round(0.0009)}", Double.class), 0.1);

    Assert.assertEquals(-1.0, eval.eval(variables, "${math:round(-0.999)}", Double.class), 0.1);
    Assert.assertEquals(0.0, eval.eval(variables, "${math:round(-0.0009)}", Double.class), 0.1);
  }
}
