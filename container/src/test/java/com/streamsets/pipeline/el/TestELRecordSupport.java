/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.record.RecordImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestELRecordSupport {

  @Test
  public void testRecordFunctions() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELRecordSupport.registerRecordFunctions(eval);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Record record = new RecordImpl("s", "id", null, null);
    record.set(Field.create(1));

    ELRecordSupport.setRecordInContext(variables, record);

    Assert.assertTrue(eval.eval(variables, "${record:type('') eq INTEGER}", Boolean.class));
    Assert.assertTrue(eval.eval(variables, "${record:value('') eq 1}", Boolean.class));
    Assert.assertNull(eval.eval(variables, "${record:value('/x')}"));


    Map<String, Field> map = new HashMap<>();
    map.put("a", Field.create(1));
    record.set(Field.create(map));
    Assert.assertEquals(Field.Type.INTEGER, eval.eval(variables, "${record:type('/a')}"));
    Assert.assertEquals(1, eval.eval(variables, "${record:value('/a')}"));
    Assert.assertNull(eval.eval(variables, "${record:value('/x')}"));
  }

}
