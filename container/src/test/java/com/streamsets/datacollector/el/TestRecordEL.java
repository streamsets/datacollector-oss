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
import com.streamsets.datacollector.definition.ELDefinitionExtractor;
import com.streamsets.datacollector.record.EventRecordImpl;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.el.RecordEL;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRecordEL {
  private ELDefinitionExtractor elDefinitionExtractor = ConcreteELDefinitionExtractor.get();

  @Test
  public void testRecordFunctions() throws Exception {
    ELEvaluator eval = new ELEvaluator("testRecordFunctions", elDefinitionExtractor, RecordEL.class);
    ELVariables variables = new ELVariables();

    Record.Header header = Mockito.mock(Record.Header.class);
    Mockito.when(header.getSourceId()).thenReturn("id");
    Mockito.when(header.getStageCreator()).thenReturn("creator");
    Mockito.when(header.getStagesPath()).thenReturn("path");
    Mockito.when(header.getAttribute(Mockito.eq("foo"))).thenReturn("bar");
    Record record = Mockito.mock(Record.class);
    Mockito.when(record.getHeader()).thenReturn(header);
    Mockito.when(record.get(Mockito.eq(""))).thenReturn(Field.create(1));
    Mockito.when(record.get(Mockito.eq("/x"))).thenReturn(null);
    Mockito.when(record.has(Mockito.eq("/x"))).thenReturn(true);
    Mockito.when(record.has(Mockito.eq("/y"))).thenReturn(false);
    Mockito.when(record.get(Mockito.eq("/a"))).thenReturn(Field.create("A"));
    Mockito.when(record.get(Mockito.eq("/null"))).thenReturn(Field.create((String)null));

    RecordEL.setRecordInContext(variables, record);

    Assert.assertTrue(eval.eval(variables, "${record:type('') eq NUMBER}", Boolean.class));
    Assert.assertTrue(eval.eval(variables, "${record:value('') eq 1}", Boolean.class));
    Assert.assertNull(eval.eval(variables, "${record:value('/x')}", Object.class));
    Assert.assertEquals("id", eval.eval(variables, "${record:id()}", String.class));
    Assert.assertEquals("creator", eval.eval(variables, "${record:creator()}", String.class));
    Assert.assertEquals("path", eval.eval(variables, "${record:path()}", String.class));
    Assert.assertEquals(true, eval.eval(variables, "${record:exists('/x')}", boolean.class));
    Assert.assertEquals(false, eval.eval(variables, "${record:exists('/y')}", boolean.class));
    Assert.assertEquals("bar", eval.eval(variables, "${record:attribute('foo')}", String.class));
    Assert.assertEquals("dummy", eval.eval(variables, "${record:valueOrDefault('/z', 'dummy')}", Object.class));
    Assert.assertEquals("A", eval.eval(variables, "${record:valueOrDefault('/a', 'dummy')}", Object.class));
    Assert.assertEquals("dummy", eval.eval(variables, "${record:valueOrDefault('/null', 'dummy')}", Object.class));
  }

  @Test
  public void testErrorRecordFunctions() throws Exception {
    ELEvaluator eval = new ELEvaluator("testErrorRecordFunctions", elDefinitionExtractor, RecordEL.class);

    ELVariables variables = new ELVariables();

    String stackTrace = new IOException("Error Record Function Stack Trace Test").toString();

    Record.Header header = Mockito.mock(Record.Header.class);
    Mockito.when(header.getErrorStage()).thenReturn("stage");
    Mockito.when(header.getErrorStageLabel()).thenReturn("label");
    Mockito.when(header.getErrorCode()).thenReturn("code");
    Mockito.when(header.getErrorMessage()).thenReturn("message");
    Mockito.when(header.getErrorDataCollectorId()).thenReturn("collector");
    Mockito.when(header.getErrorPipelineName()).thenReturn("pipeline");
    Mockito.when(header.getErrorJobId()).thenReturn("jobId");
    Mockito.when(header.getErrorJobName()).thenReturn("jobName");
    Mockito.when(header.getErrorTimestamp()).thenReturn(10L);
    Mockito.when(header.getErrorStackTrace()).thenReturn(stackTrace);
    Record record = Mockito.mock(Record.class);
    Mockito.when(record.getHeader()).thenReturn(header);

    RecordEL.setRecordInContext(variables, record);

    Assert.assertEquals("stage", eval.eval(variables, "${record:errorStage()}", String.class));
    Assert.assertEquals("label", eval.eval(variables, "${record:errorStageLabel()}", String.class));
    Assert.assertEquals("code", eval.eval(variables, "${record:errorCode()}", String.class));
    Assert.assertEquals("message", eval.eval(variables, "${record:errorMessage()}", String.class));
    Assert.assertEquals(stackTrace, eval.eval(variables, "${record:errorStackTrace()}", String.class));
    Assert.assertEquals("collector", eval.eval(variables, "${record:errorCollectorId()}", String.class));
    Assert.assertEquals("pipeline", eval.eval(variables, "${record:errorPipeline()}", String.class));
    Assert.assertEquals("jobId", eval.eval(variables, "${record:errorJobId()}", String.class));
    Assert.assertEquals("jobName", eval.eval(variables, "${record:errorJobName()}", String.class));
    Assert.assertEquals(10L, (long)eval.eval(variables, "${record:errorTime()}", Long.class));
  }

  @Test
  public void testDFunctions() throws Exception {
    ELEvaluator eval = new ELEvaluator("testDFunctions", elDefinitionExtractor, RecordEL.class);

    ELVariables variables = new ELVariables();

    Map<String, Field> col1 = new HashMap<>();
    col1.put("header", Field.create("a"));
    col1.put("value", Field.create("A"));
    Map<String, Field> col2 = new HashMap<>();
    col2.put("header", Field.create("b"));
    col2.put("value", Field.create("B"));
    Map<String, Field> col3 = new HashMap<>();
    col3.put("header", Field.create("c"));
    col3.put("value", Field.create("C"));
    Map<String, Field> col4 = new HashMap<>();
    col4.put("header", Field.create("a"));
    col4.put("value", Field.create("X"));
    List<Field> list = Arrays.asList(Field.create(col1), Field.create(col2), Field.create(col3), Field.create(col4));
    Record record = Mockito.mock(Record.class);
    Mockito.when(record.get()).thenReturn(Field.create(list));

    RecordEL.setRecordInContext(variables, record);

    Assert.assertEquals("A", eval.eval(variables, "${record:dValue('a')}", String.class));
    Assert.assertEquals("C", eval.eval(variables, "${record:dValue('c')}", String.class));
    Assert.assertEquals("", eval.eval(variables, "${record:dValue('x')}", String.class));

    Assert.assertEquals(0, (int) eval.eval(variables, "${record:dIndex('a')}", Integer.class));
    Assert.assertEquals(2, (int) eval.eval(variables, "${record:dIndex('c')}", Integer.class));
    Assert.assertEquals(-1, (int) eval.eval(variables, "${record:dIndex('x')}", Integer.class));

    Assert.assertEquals("A", eval.eval(variables, "${record:dValueAt(0)}", String.class));
    Assert.assertEquals("C", eval.eval(variables, "${record:dValueAt(2)}", String.class));
    Assert.assertEquals("", eval.eval(variables, "${record:dValueAt(4)}", String.class));
    Assert.assertEquals("", eval.eval(variables, "${record:dValueAt(-1)}", String.class));

    Assert.assertTrue(eval.eval(variables, "${record:dExists('a')}", Boolean.class));
    Assert.assertTrue(eval.eval(variables, "${record:dExists('c')}", Boolean.class));
    Assert.assertFalse(eval.eval(variables, "${record:dExists('x')}", Boolean.class));

    Assert.assertTrue(eval.eval(variables, "${record:dIsDupHeader('a')}", Boolean.class));
    Assert.assertFalse(eval.eval(variables, "${record:dIsDupHeader('c')}", Boolean.class));
    Assert.assertFalse(eval.eval(variables, "${record:dIsDupHeader('x')}", Boolean.class));

    Map<String, Field> map = new HashMap<>();
    map.put(col1.get("header").getValueAsString(), col1.get("value"));
    map.put(col2.get("header").getValueAsString(), col2.get("value"));
    map.put(col3.get("header").getValueAsString(), col3.get("value"));
    map.put(col4.get("header").getValueAsString(), col4.get("value"));
    Assert.assertEquals(map, eval.eval(variables, "${record:dToMap()}", Map.class));

    Assert.assertTrue(eval.eval(variables, "${record:dHasDupHeaders()}", Boolean.class));

    record.get().getValueAsList().remove(3);
    Assert.assertFalse(eval.eval(variables, "${record:dHasDupHeaders()}", Boolean.class));

    col1.remove("header");
    col2.remove("header");
    col3.remove("header");
    col4.remove("header");
    list = Arrays.asList(Field.create(col1), Field.create(col2), Field.create(col3), Field.create(col4));
    record = Mockito.mock(Record.class);
    Mockito.when(record.get()).thenReturn(Field.create(list));
    RecordEL.setRecordInContext(variables, record);
    map = new HashMap<>();
    map.put("0", col1.get("value"));
    map.put("1", col2.get("value"));
    map.put("2", col3.get("value"));
    map.put("3", col4.get("value"));
    Assert.assertEquals(map, eval.eval(variables, "${record:dToMap()}", Map.class));

  }

  @Test
  public void testFieldAttributeFunctions() throws Exception {
    ELEvaluator eval = new ELEvaluator("testFieldAttributeFunctions", elDefinitionExtractor, RecordEL.class);

    ELVariables vars = new ELVariables();

    Map<String, Field> fieldAMap = new HashMap<>();
    Field aFirstField = Field.create("1");
    aFirstField.setAttribute("attr1", "attrVal1");
    aFirstField.setAttribute("attr2", "attrVal2");
    fieldAMap.put("first", aFirstField);

    Field aSecondField = Field.create("2");
    aSecondField.setAttribute("attr3", "attrVal3");
    fieldAMap.put("second", aSecondField);

    Map<String, Field> rootFields = new HashMap<>();
    Field aField = Field.create(fieldAMap);
    aField.setAttribute("attr10", "attrVal10");
    rootFields.put("A", aField);

    Field bField = Field.create("b_value");
    bField.setAttribute("attr20", "attrVal20");
    bField.setAttribute("attr21", "attrVal21");
    rootFields.put("B", bField);

    Record record = Mockito.mock(Record.class);
    Mockito.when(record.get()).thenReturn(Field.create(rootFields));
    Mockito.when(record.get("/A")).thenReturn(aField);
    Mockito.when(record.get("/A/first")).thenReturn(aFirstField);
    Mockito.when(record.get("/A/second")).thenReturn(aSecondField);
    Mockito.when(record.get("/B")).thenReturn(bField);

    RecordEL.setRecordInContext(vars, record);

    assertEquals("attrVal1", eval.eval(vars, "${record:fieldAttribute('/A/first', 'attr1')}", String.class));
    assertEquals("attrVal2", eval.eval(vars, "${record:fieldAttribute('/A/first', 'attr2')}", String.class));
    assertEquals("", eval.eval(vars, "${record:fieldAttribute('/A/first', 'attr3')}", String.class));
    assertEquals(
        "default",
        eval.eval(vars, "${record:fieldAttributeOrDefault('/A/first', 'attr3', 'default')}",
            String.class)
    );
    assertEquals(
        "attrVal3",
        eval.eval(vars, "${record:fieldAttributeOrDefault('/A/second', 'attr3', 'default')}",
            String.class)
    );
    assertEquals("attrVal10", eval.eval(vars, "${record:fieldAttribute('/A', 'attr10')}", String.class));
    assertEquals("attrVal20", eval.eval(vars, "${record:fieldAttribute('/B', 'attr20')}", String.class));
    assertEquals("attrVal21", eval.eval(vars, "${record:fieldAttribute('/B', 'attr21')}", String.class));
    assertEquals(
        "default2",
        eval.eval(vars, "${record:fieldAttributeOrDefault('/XYZ', 'attr21', 'default2')}",
            String.class)
    );
  }

  @Test
  public void testEventMethods() throws Exception {
    ELEvaluator eval = new ELEvaluator("testEventMethods", elDefinitionExtractor, RecordEL.class);
    ELVariables vars = new ELVariables();

    EventRecord event = new EventRecordImpl("custom-type", 1, "stage", "id", null, null);
    RecordEL.setRecordInContext(vars, event);

    assertEquals("custom-type", eval.eval(vars, "${record:eventType()}", String.class));
    assertEquals("1", eval.eval(vars, "${record:eventVersion()}", String.class));
    assertTrue(eval.eval(vars, "${record:eventType() == 'custom-type'}", Boolean.class));
    assertTrue(eval.eval(vars, "${record:eventVersion() == '1'}", Boolean.class));

    Record record = new RecordImpl("stage", "id", null, null);
    RecordEL.setRecordInContext(vars, record);
    assertEquals("", eval.eval(vars, "${record:eventType()}", String.class));
    assertEquals("", eval.eval(vars, "${record:eventVersion()}", String.class));
    assertTrue(eval.eval(vars, "${record:eventType() == NULL}", Boolean.class));
    assertTrue(eval.eval(vars, "${record:eventVersion() == NULL}", Boolean.class));
  }
}
