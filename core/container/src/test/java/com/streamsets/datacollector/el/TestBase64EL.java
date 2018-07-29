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
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.el.Base64EL;
import com.streamsets.pipeline.lib.el.RecordEL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestBase64EL {
  private static final String testString = "http://abcdef.ghijkl:8080/this is a test/?abc=def";
  private ELEvaluator eval;
  private ELVariables variables;

  @Before
  public void setUp() {
    eval = new ELEvaluator("testBase64Encode", ConcreteELDefinitionExtractor.get(), Base64EL.class, RecordEL.class);
    variables = new ELVariables();

    Record record = new RecordImpl("test", "test", null, null);
    record.set(Field.create(testString.getBytes()));
    RecordEL.setRecordInContext(variables, record);
  }

  @Test
  public void testBase64Encode() throws Exception {
    Assert.assertEquals("aHR0cDovL2FiY2RlZi5naGlqa2w6ODA4MC90aGlzIGlzIGEgdGVzdC8/YWJjPWRlZg==", eval.eval(variables,
        "${base64:encodeString('http://abcdef.ghijkl:8080/this is a test/?abc=def', false, 'UTF-8')}",
        String.class
    ));

    Assert.assertEquals("aHR0cDovL2FiY2RlZi5naGlqa2w6ODA4MC90aGlzIGlzIGEgdGVzdC8_YWJjPWRlZg", eval.eval(variables,
        "${base64:encodeString('http://abcdef.ghijkl:8080/this is a test/?abc=def', true, 'UTF-8')}",
        String.class
    ));
  }

  @Test
  public void testBase64Decode() throws Exception {
    ELEvaluator eval = new ELEvaluator("testBase64Decode", ConcreteELDefinitionExtractor.get(), Base64EL.class);
    ELVariables variables = new ELVariables();

    Assert.assertEquals(testString, eval.eval(
        variables,
        "${base64:decodeString('aHR0cDovL2FiY2RlZi5naGlqa2w6ODA4MC90aGlzIGlzIGEgdGVzdC8/YWJjPWRlZg==', 'UTF-8')}",
        String.class
    ));

    Assert.assertArrayEquals(testString.getBytes(), eval.eval(
        variables,
        "${base64:decodeBytes('aHR0cDovL2FiY2RlZi5naGlqa2w6ODA4MC90aGlzIGlzIGEgdGVzdC8_YWJjPWRlZg')}",
        byte[].class
    ));
  }
}
