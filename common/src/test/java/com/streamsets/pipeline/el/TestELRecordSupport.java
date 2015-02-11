/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestELRecordSupport {

  @Test
  public void testRecordFunctions() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELRecordSupport.registerRecordFunctions(eval);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Record.Header header = Mockito.mock(Record.Header.class);
    Mockito.when(header.getSourceId()).thenReturn("id");
    Mockito.when(header.getStageCreator()).thenReturn("creator");
    Mockito.when(header.getStagesPath()).thenReturn("path");
    Record record = Mockito.mock(Record.class);
    Mockito.when(record.getHeader()).thenReturn(header);
    Mockito.when(record.get(Mockito.eq(""))).thenReturn(Field.create(1));
    Mockito.when(record.get(Mockito.eq("/x"))).thenReturn(null);

    ELRecordSupport.setRecordInContext(variables, record);

    Assert.assertTrue(eval.eval(variables, "${record:type('') eq INTEGER}", Boolean.class));
    Assert.assertTrue(eval.eval(variables, "${record:value('') eq 1}", Boolean.class));
    Assert.assertNull(eval.eval(variables, "${record:value('/x')}"));
    Assert.assertEquals("id", eval.eval(variables, "${record:id()}"));
    Assert.assertEquals("creator", eval.eval(variables, "${record:creator()}"));
    Assert.assertEquals("path", eval.eval(variables, "${record:path()}"));
  }

  @Test
  public void testErrorRecordFunctions() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELRecordSupport.registerRecordFunctions(eval);


    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Record.Header header = Mockito.mock(Record.Header.class);
    Mockito.when(header.getErrorStage()).thenReturn("stage");
    Mockito.when(header.getErrorCode()).thenReturn("code");
    Mockito.when(header.getErrorMessage()).thenReturn("message");
    Mockito.when(header.getErrorDataCollectorId()).thenReturn("collector");
    Mockito.when(header.getErrorPipelineName()).thenReturn("pipeline");
    Mockito.when(header.getErrorTimestamp()).thenReturn(10l);
    Record record = Mockito.mock(Record.class);
    Mockito.when(record.getHeader()).thenReturn(header);

    ELRecordSupport.setRecordInContext(variables, record);

    Assert.assertEquals("stage", eval.eval(variables, "${error:stage()}"));
    Assert.assertEquals("code", eval.eval(variables, "${error:code()}"));
    Assert.assertEquals("message", eval.eval(variables, "${error:message()}"));
    Assert.assertEquals("collector", eval.eval(variables, "${error:collectorId()}"));
    Assert.assertEquals("pipeline", eval.eval(variables, "${error:pipeline()}"));
    Assert.assertEquals(10l, eval.eval(variables, "${error:time()}"));
  }


}
