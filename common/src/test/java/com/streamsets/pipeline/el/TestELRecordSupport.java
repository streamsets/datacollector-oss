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

    Record record = Mockito.mock(Record.class);
    Mockito.when(record.get(Mockito.eq(""))).thenReturn(Field.create(1));
    Mockito.when(record.get(Mockito.eq("/x"))).thenReturn(null);

    ELRecordSupport.setRecordInContext(variables, record);

    Assert.assertTrue(eval.eval(variables, "${record:type('') eq INTEGER}", Boolean.class));
    Assert.assertTrue(eval.eval(variables, "${record:value('') eq 1}", Boolean.class));
    Assert.assertNull(eval.eval(variables, "${record:value('/x')}"));
  }

}
