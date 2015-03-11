/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.el.RecordEL;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestRecordEL {

  @Test
  public void testRecordFunctions() throws Exception {
    ELEvaluator eval = new ELEvaluator("testRecordFunctions", RecordEL.class);
    ELVariables variables = new ELVariables();

    Record.Header header = Mockito.mock(Record.Header.class);
    Mockito.when(header.getSourceId()).thenReturn("id");
    Mockito.when(header.getStageCreator()).thenReturn("creator");
    Mockito.when(header.getStagesPath()).thenReturn("path");
    Record record = Mockito.mock(Record.class);
    Mockito.when(record.getHeader()).thenReturn(header);
    Mockito.when(record.get(Mockito.eq(""))).thenReturn(Field.create(1));
    Mockito.when(record.get(Mockito.eq("/x"))).thenReturn(null);
    Mockito.when(record.has(Mockito.eq("/x"))).thenReturn(true);
    Mockito.when(record.has(Mockito.eq("/y"))).thenReturn(false);

    RecordEL.setRecordInContext(variables, record);

    Assert.assertTrue(eval.eval(variables, "${record:type('') eq NUMBER}", Boolean.class));
    Assert.assertTrue(eval.eval(variables, "${record:value('') eq 1}", Boolean.class));
    Assert.assertNull(eval.eval(variables, "${record:value('/x')}", Object.class));
    Assert.assertEquals("id", eval.eval(variables, "${record:id()}", String.class));
    Assert.assertEquals("creator", eval.eval(variables, "${record:creator()}", String.class));
    Assert.assertEquals("path", eval.eval(variables, "${record:path()}", String.class));
    Assert.assertEquals(true, eval.eval(variables, "${record:exists('/x')}", boolean.class));
    Assert.assertEquals(false, eval.eval(variables, "${record:exists('/y')}", boolean.class));
  }

  @Test
  public void testErrorRecordFunctions() throws Exception {
    ELEvaluator eval = new ELEvaluator("testErrorRecordFunctions", RecordEL.class);

    ELVariables variables = new ELVariables();

    Record.Header header = Mockito.mock(Record.Header.class);
    Mockito.when(header.getErrorStage()).thenReturn("stage");
    Mockito.when(header.getErrorCode()).thenReturn("code");
    Mockito.when(header.getErrorMessage()).thenReturn("message");
    Mockito.when(header.getErrorDataCollectorId()).thenReturn("collector");
    Mockito.when(header.getErrorPipelineName()).thenReturn("pipeline");
    Mockito.when(header.getErrorTimestamp()).thenReturn(10l);
    Record record = Mockito.mock(Record.class);
    Mockito.when(record.getHeader()).thenReturn(header);

    RecordEL.setRecordInContext(variables, record);

    Assert.assertEquals("stage", eval.eval(variables, "${error:stage()}", String.class));
    Assert.assertEquals("code", eval.eval(variables, "${error:code()}", String.class));
    Assert.assertEquals("message", eval.eval(variables, "${error:message()}", String.class));
    Assert.assertEquals("collector", eval.eval(variables, "${error:collectorId()}", String.class));
    Assert.assertEquals("pipeline", eval.eval(variables, "${error:pipeline()}", String.class));
    Assert.assertEquals(10l, (long)eval.eval(variables, "${error:time()}", Long.class));
  }

}
