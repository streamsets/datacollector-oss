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

import com.streamsets.datacollector.definition.ConcreteELDefinitionExtractor;
import com.streamsets.datacollector.definition.ELDefinitionExtractor;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.lib.el.AggregationEL;
import com.streamsets.testing.Matchers;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class TestAggregationEL {
  private ELDefinitionExtractor elDefinitionExtractor = ConcreteELDefinitionExtractor.get();

  private static ELVariables makeTestVariables() {
    final ELVariables variables = new ELVariables();
    variables.addVariable("intFields", makeFields(3, 2, 1));
    variables.addVariable("doubleFields", makeFields(0.5d, 2.8d, 6.2d));
    variables.addVariable("decimalFields", makeFields(
        new BigDecimal("19.75"),
        new BigDecimal("-8.05"),
        new BigDecimal("471.2")));
    return variables;
  }

  private static List<Field> makeFields(Double... values) {
    return Arrays.asList(values).stream().map(value -> Field.create(value)).collect(Collectors.toList());
  }

  private static List<Field> makeFields(Integer... values) {
    return Arrays.asList(values).stream().map(value -> Field.create(value)).collect(Collectors.toList());
  }

  private static List<Field> makeFields(BigDecimal... values) {
    return Arrays.asList(values).stream().map(value -> Field.create(value)).collect(Collectors.toList());
  }

  @Test
  public void testSums() throws Exception {
    final ELEvaluator eval = new ELEvaluator("testSums", elDefinitionExtractor, AggregationEL.class);

    final ELVariables variables = makeTestVariables();

    final Field result1 = eval.eval(variables, "${sum(intFields)}", Field.class);
    assertThat(result1, Matchers.fieldWithValue(6l));

    final Field result2 = eval.eval(variables, "${sum(doubleFields)}", Field.class);
    assertThat(result2, Matchers.fieldWithValue(9.5d));

    final Field result3 = eval.eval(variables, "${sum(decimalFields)}", Field.class);
    assertThat(result3, Matchers.fieldWithValue(new BigDecimal("482.90")));
  }

  @Test
  public void testMin() throws Exception {
    final ELEvaluator eval = new ELEvaluator("testMin", elDefinitionExtractor, AggregationEL.class);

    final ELVariables variables = makeTestVariables();

    final Field result1 = eval.eval(variables, "${min(intFields)}", Field.class);
    assertThat(result1, Matchers.fieldWithValue(1l));

    final Field result2 = eval.eval(variables, "${min(doubleFields)}", Field.class);
    assertThat(result2, Matchers.fieldWithValue(0.5d));

    final Field result3 = eval.eval(variables, "${min(decimalFields)}", Field.class);
    assertThat(result3, Matchers.fieldWithValue(new BigDecimal("-8.05")));
  }

  @Test
  public void testMax() throws Exception {
    final ELEvaluator eval = new ELEvaluator("testMax", elDefinitionExtractor, AggregationEL.class);

    final ELVariables variables = makeTestVariables();

    final Field result1 = eval.eval(variables, "${max(intFields)}", Field.class);
    assertThat(result1, Matchers.fieldWithValue(3l));

    final Field result2 = eval.eval(variables, "${max(doubleFields)}", Field.class);
    assertThat(result2, Matchers.fieldWithValue(6.2d));

    final Field result3 = eval.eval(variables, "${max(decimalFields)}", Field.class);
    assertThat(result3, Matchers.fieldWithValue(new BigDecimal("471.2")));
  }
}