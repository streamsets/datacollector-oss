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
package com.streamsets.datacollector.definition;

import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.pipeline.api.ElConstant;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestELDefinitionExtractor {

  public static class Empty {
  }

  public static class Ok {

    @ElFunction(prefix = "p", name = "f", description = "ff", implicitOnly = true)
    public static String f(@ElParam("x") int x) {
      return null;
    }

    @ElConstant(name = "C", description = "CC")
    public static final String C = "c";

  }

  public static class Fail1 {

    @ElFunction(prefix = "p", name = "f")
    public String f() {
      return null;
    }

    @ElConstant(name = "C", description = "CC")
    public final String C = "c";

  }

  public static class Fail2 {

    @ElFunction(prefix = "p", name = "f")
    private static String f() {
      return null;
    }

    @ElConstant(name = "C", description = "CC")
    protected static final String C = "c";

  }

  public static class Fail3 {

    @ElFunction(prefix = "p", name = "f")
    public static String f(String param) {
      return null;
    }

  }

  @Test
  public void testExtractionEmpty() {
    Assert.assertTrue(ConcreteELDefinitionExtractor.get().extractFunctions(ImmutableSet.<Class>of(Empty.class), "").isEmpty());
    Assert.assertTrue(ConcreteELDefinitionExtractor.get().extractConstants(ImmutableSet.<Class>of(Empty.class), "").isEmpty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExtractionFail1Function() {
    ConcreteELDefinitionExtractor.get().extractFunctions(new Class[]{Fail1.class}, "x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExtractionFail2Function() {
    ConcreteELDefinitionExtractor.get().extractFunctions(new Class[]{Fail2.class}, "x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExtractionFail1Constant() {
    ConcreteELDefinitionExtractor.get().extractConstants(new Class[]{Fail1.class}, "x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExtractionFail2Constant() {
    ConcreteELDefinitionExtractor.get().extractConstants(new Class[]{Fail2.class}, "x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExtractionFail3MissingElParam() {
    ConcreteELDefinitionExtractor.get().extractFunctions(new Class[]{Fail3.class}, "x");
  }

  @Test
  public void testExtractionFunctions() {
    List<ElFunctionDefinition> functions =
        ConcreteELDefinitionExtractor.get().extractFunctions(ImmutableSet.<Class>of(Ok.class), "x");
    Assert.assertEquals(1, functions.size());
    Assert.assertEquals("p:f", functions.get(0).getName());
    Assert.assertEquals("p", functions.get(0).getGroup());
    Assert.assertEquals("ff", functions.get(0).getDescription());
    Assert.assertTrue(functions.get(0).isImplicitOnly());
    Assert.assertEquals(String.class.getSimpleName(), functions.get(0).getReturnType());
    Assert.assertEquals(1, functions.get(0).getElFunctionArgumentDefinition().size());
    Assert.assertEquals("x", functions.get(0).getElFunctionArgumentDefinition().get(0).getName());
    Assert.assertEquals(Integer.TYPE.getSimpleName(),
                        functions.get(0).getElFunctionArgumentDefinition().get(0).getType());
  }

  @Test
  public void testExtractionConstants() {
    List<ElConstantDefinition> constants =
        ConcreteELDefinitionExtractor.get().extractConstants(ImmutableSet.<Class>of(Ok.class), "x");
    Assert.assertEquals(1, constants.size());
    Assert.assertEquals("C", constants.get(0).getName());
    Assert.assertEquals("CC", constants.get(0).getDescription());
    Assert.assertEquals(String.class.getSimpleName(), constants.get(0).getReturnType());
  }
}
