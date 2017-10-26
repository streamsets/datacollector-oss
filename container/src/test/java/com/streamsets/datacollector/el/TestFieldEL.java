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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.el.RecordEL;

import com.streamsets.pipeline.lib.el.FieldEL;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFieldEL {

  @Test
  public void testFieldFunctions() throws Exception {
    ELEvaluator eval = new ELEvaluator("testRecordFunctions", FieldEL.class);
    ELVariables variables = new ELVariables();

    final int expectedIntFieldValue = 1;
    final String expectedAFieldValue = "A";
    final String expectedAFieldPath = "/a";

    Record record = Mockito.mock(Record.class);
    Mockito.when(record.get(Mockito.eq(""))).thenReturn(Field.create(expectedIntFieldValue));
    Mockito.when(record.get(Mockito.eq("/x"))).thenReturn(null);
    Mockito.when(record.has(Mockito.eq("/x"))).thenReturn(true);
    Mockito.when(record.has(Mockito.eq("/y"))).thenReturn(false);
    Mockito.when(record.get(Mockito.eq(expectedAFieldPath))).thenReturn(Field.create(expectedAFieldValue));
    Mockito.when(record.get(Mockito.eq("/null"))).thenReturn(Field.create((String) null));

    RecordEL.setRecordInContext(variables, record);
    Field aField = record.get(expectedAFieldPath);

    FieldEL.setFieldInContext(variables, expectedAFieldPath, aField);

    final Field.Type typeEval = eval.eval(variables, "${f:type()}", Field.Type.class);
    Assert.assertNotNull(typeEval);
    Assert.assertEquals(Field.Type.STRING, typeEval);

    final Object valueEval = eval.eval(variables, "${f:value()}", Object.class);
    Assert.assertNotNull(valueEval);
    Assert.assertEquals(expectedAFieldValue, valueEval);

    final String pathEval = eval.eval(variables, "${f:path()}", String.class);
    Assert.assertNotNull(pathEval);
    Assert.assertEquals(expectedAFieldPath, pathEval);
  }
}