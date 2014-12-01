/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
