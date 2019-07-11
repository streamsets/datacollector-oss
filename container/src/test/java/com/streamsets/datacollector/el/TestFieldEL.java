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
import com.streamsets.datacollector.record.HeaderImpl;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.FieldEL;
import com.streamsets.testing.fieldbuilder.MapFieldBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestFieldEL {

  @Test
  public void testFieldFunctions() {
    final MapFieldBuilder builder = MapFieldBuilder.builder();
    final int aFieldValue = 1;
    final String aFieldName = "a";
    final String sisterFieldName = "sister";
    final long sisterFieldValue = -89l;
    final String baseFieldName = "fields";
    final int indexWithinParent = 3;
    builder.startListMap(baseFieldName)
        .add(aFieldName, aFieldValue)
        .add(sisterFieldName, sisterFieldValue)
        .end(); // end fields map

    // cannot use RecordCreator here, since that is from sdk module, and depending on it from here would create
    // a circular dependency
    final Record record = new RecordImpl(new HeaderImpl(), builder.build());

    final ELEvaluator eval = new ELEvaluator("testFieldFunctions", ConcreteELDefinitionExtractor.get(), FieldEL.class);
    final ELVariables variables = new ELVariables();

    RecordEL.setRecordInContext(variables, record);
    final String parentFieldPath = "/" + baseFieldName;
    final String sisterFieldPath = parentFieldPath + "/" + sisterFieldName;
    final String aFieldPath = parentFieldPath + "/" + aFieldName;

    final Field parentField = record.get(parentFieldPath);
    final Field aField = record.get(aFieldPath);

    FieldEL.setFieldInContext(variables, aFieldPath, aFieldName, aField, parentFieldPath, parentField, indexWithinParent);

    assertNonNullEval(eval, variables, "${f:type()}", Field.Type.INTEGER, Field.Type.class);

    assertNonNullEval(eval, variables, "${f:value()}", aFieldValue, Object.class);

    assertNonNullEval(eval, variables, "${f:path()}", aFieldPath, String.class);

    // indexWithinParent is just set to an arbitrary value here; we are not testing that it's set correctly for actual
    // record traversal, just that it's evaluated correctly by the EL functions
    assertNonNullEval(eval, variables, "${f:index()}", indexWithinParent, Integer.class);

    assertNonNullEval(eval, variables, "${f:parentPath()}", parentFieldPath, String.class);

    assertNonNullEval(eval, variables, "${f:parent()}", parentField, Field.class);

    assertNonNullEval(
        eval,
        variables,
        String.format("${f:hasSiblingWithName('%s')}", sisterFieldName),
        true,
        Boolean.class
    );

    assertNonNullEval(
        eval,
        variables,
        String.format("${f:getSiblingWithName('%s')}", sisterFieldName),
        record.get(sisterFieldPath),
        Field.class
    );

    assertNonNullEval(
        eval,
        variables,
        String.format("${f:hasSiblingWithValue('%s', %d)}", sisterFieldName, sisterFieldValue),
        true,
        Boolean.class
    );
  }

  static <T> void assertNonNullEval(
      ELEvaluator eval,
      ELVariables elVars,
      String expression,
      T expected,
      Class<T> expectedClass
  ) {
    final T evalResult = eval.eval(
        elVars,
        expression,
        expectedClass
    );
    Assert.assertNotNull(evalResult);
    Assert.assertEquals(expected, evalResult);
  }
}