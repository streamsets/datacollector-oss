/*
 * Copyright 2018 StreamSets Inc.
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

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.definition.ConcreteELDefinitionExtractor;
import com.streamsets.datacollector.definition.ELDefinitionExtractor;
import com.streamsets.pipeline.lib.el.CollectionEL;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestCollectionEL {
  private ELDefinitionExtractor elDefinitionExtractor = ConcreteELDefinitionExtractor.get();

  @Test
  public void testContains() throws Exception {
    ELEvaluator eval = new ELEvaluator("testContains", elDefinitionExtractor, CollectionEL.class);
    ELVariables variables = new ELVariables();

    // Positive cases
    variables.addVariable("list", ImmutableList.of("a", "b"));
    assertEquals(true, eval.eval(variables,"${collection:contains(list, 'a')}", Boolean.class));
    assertEquals(true, eval.eval(variables,"${collection:contains(list, 'b')}", Boolean.class));

    // Negative cases
    assertEquals(false, eval.eval(variables,"${collection:contains(list, 'c')}", Boolean.class));
    assertEquals(false, eval.eval(variables,"${collection:contains(list, '')}", Boolean.class));
  }

  @Test
  public void testFilterByRegExp() throws Exception {
    ELEvaluator eval = new ELEvaluator("testFilterByRegExp", elDefinitionExtractor, CollectionEL.class);
    ELVariables variables = new ELVariables();
    List output;

    // Positive cases
    variables.addVariable("list", ImmutableList.of("a", "b"));
    output = eval.eval(variables,"${collection:filterByRegExp(list, 'a')}", List.class);
    assertEquals(1, output.size());

    // Negative cases
    output = eval.eval(variables,"${collection:filterByRegExp(list, 'not-there')}", List.class);
    assertEquals(0, output.size());
  }

  @Test
  public void testSize() throws Exception {
    ELEvaluator eval = new ELEvaluator("testSize", elDefinitionExtractor, CollectionEL.class);
    ELVariables variables = new ELVariables();

    // Positive cases
    variables.addVariable("list", ImmutableList.of("a", "b"));
    assertEquals(2, (int)eval.eval(variables,"${collection:size(list)}", Integer.class));
  }

  @Test
  public void testGet() throws Exception {
    ELEvaluator eval = new ELEvaluator("testGet", elDefinitionExtractor, CollectionEL.class);
    ELVariables variables = new ELVariables();

    // Positive cases
    variables.addVariable("list", ImmutableList.of("a", "b"));
    assertEquals("a", eval.eval(variables,"${collection:get(list, 0)}", String.class));
    assertEquals("b", eval.eval(variables,"${collection:get(list, 1)}", String.class));
    assertEquals("", eval.eval(variables,"${collection:get(list, 2)}", String.class));
  }
}
