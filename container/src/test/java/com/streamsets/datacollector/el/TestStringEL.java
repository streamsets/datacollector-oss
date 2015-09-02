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
package com.streamsets.datacollector.el;

import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.lib.el.StringEL;

import org.junit.Assert;
import org.junit.Test;

public class TestStringEL {

  @Test
  public void testSubstring() throws Exception {
    ELEvaluator eval = new ELEvaluator("testSubstring", StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertEquals("StreamSets", eval.eval(variables,
      "${str:substring(\"The StreamSets Inc\", 4, 14)}", String.class));

    //End index greater than length, return beginIndex to end of string
    Assert.assertEquals("StreamSets Inc", eval.eval(variables,
      "${str:substring(\"The StreamSets Inc\", 4, 50)}", String.class));

    //Begin Index > length, return ""
    Assert.assertEquals("", eval.eval(variables,
      "${str:substring(\"The StreamSets Inc\", 50, 60)}", String.class));
  }

  @Test
  public void testSubstringNegative() throws Exception {
    ELEvaluator eval = new ELEvaluator("testSubstringNegative", StringEL.class);

    ELVariables variables = new ELVariables();

    try {
      eval.eval(variables, "${str:substring(\"The StreamSets Inc\", -1, 14)}", String.class);
      Assert.fail("ELException expected as the begin index is negative");
    } catch (ELEvalException e) {

    }

    try {
      eval.eval(variables, "${str:substring(\"The StreamSets Inc\", 0, -3)}", String.class);
      Assert.fail("ELException expected as the end index is negative");
    } catch (ELEvalException e) {

    }

    //Input is empty, return empty
    Assert.assertEquals("", eval.eval(variables, "${str:substring(\"\", 0, 5)}", String.class));
  }

  @Test
  public void testTrim() throws Exception {
    ELEvaluator eval = new ELEvaluator("testTrim", StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertEquals("StreamSets", eval.eval(variables,
      "${str:trim(\"   StreamSets  \")}", String.class));
  }

  @Test
  public void testToUpper() throws Exception {
    ELEvaluator eval = new ELEvaluator("testToUpper", StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertEquals("STREAMSETS", eval.eval(variables,
      "${str:toUpper(\"StreamSets\")}", String.class));
  }

  @Test
  public void testToLower() throws Exception {
    ELEvaluator eval = new ELEvaluator("testToLower", StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertEquals("streamsets inc", eval.eval(variables,
      "${str:toLower(\"StreamSets INC\")}", String.class));
  }

  @Test
  public void testReplace() throws Exception {
    ELEvaluator eval = new ELEvaluator("testReplace", StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertEquals("The.Streamsets.Inc", eval.eval(variables,
      "${str:replace(\"The Streamsets Inc\", ' ', '.')}", String.class));
  }

  @Test
  public void testReplaceAll() throws Exception {
    ELEvaluator eval = new ELEvaluator("testReplaceAll", StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertEquals("The Streamsets Company", eval.eval(variables,
      "${str:replaceAll(\"The Streamsets Inc\", \"Inc\", \"Company\")}", String.class));
  }

  @Test
  public void testContains() throws Exception {
    ELEvaluator eval = new ELEvaluator("testContains", StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertTrue(eval.eval(variables, "${str:contains(\"The Streamsets Inc\", \"Inc\")}", Boolean.class));
    Assert.assertFalse(eval.eval(variables, "${str:contains(\"The Streamsets Inc\", \"Incorporated\")}", Boolean.class));
  }

  @Test
  public void testStartsWith() throws Exception {
    ELEvaluator eval = new ELEvaluator("testStartsWith", StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertTrue(eval.eval(variables, "${str:startsWith(\"The Streamsets Inc\", \"The Streamsets\")}", Boolean.class));
    Assert.assertFalse(eval.eval(variables, "${str:startsWith(\"The Streamsets Inc\", \"Streamsets Inc\")}", Boolean.class));
  }

  @Test
  public void testEndsWith() throws Exception {
    ELEvaluator eval = new ELEvaluator("testEndsWith", StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertFalse(eval.eval(variables, "${str:endsWith(\"The Streamsets Inc\", \"The Streamsets\")}", Boolean.class));
    Assert.assertTrue(eval.eval(variables, "${str:endsWith(\"The Streamsets Inc\", \"Streamsets Inc\")}", Boolean.class));
  }

  @Test
  public void testTruncate() throws Exception {
    ELEvaluator eval = new ELEvaluator("testTruncate", StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertEquals("The StreamSets", eval.eval(variables,
      "${str:truncate(\"The StreamSets Inc\", 14)}", String.class));
  }

  @Test
  public void testRegExCapture() throws Exception {

    ELEvaluator eval = new ELEvaluator("testRegExCapture", StringEL.class);

    ELVariables variables = new ELVariables();

    String result = eval.eval(variables,
      "${str:regExCapture(\"2015-01-18 22:31:51,813 DEBUG ZkClient - Received event: WatchedEvent state:Disconnected\"," +
        " \"(\\\\d{4}-\\\\d{2}-\\\\d{2}) (\\\\d{2}:\\\\d{2}:\\\\d{2},\\\\d{3}) ([^ ]*) ([^ ]*) - (.*)$\", " +
        "1)}", String.class);
    Assert.assertEquals("2015-01-18", result);

    result = eval.eval(variables,
      "${str:regExCapture(\"2015-01-18 22:31:51,813 DEBUG ZkClient - Received event: WatchedEvent state:Disconnected\"," +
        " \"(\\\\d{4}-\\\\d{2}-\\\\d{2}) (\\\\d{2}:\\\\d{2}:\\\\d{2},\\\\d{3}) ([^ ]*) ([^ ]*) - (.*)$\", " +
        "2)}", String.class);
    Assert.assertEquals("22:31:51,813", result);

    result = eval.eval(variables,
      "${str:regExCapture(\"2015-01-18 22:31:51,813 DEBUG ZkClient - Received event: WatchedEvent state:Disconnected\"," +
        " \"(\\\\d{4}-\\\\d{2}-\\\\d{2}) (\\\\d{2}:\\\\d{2}:\\\\d{2},\\\\d{3}) ([^ ]*) ([^ ]*) - (.*)$\", " +
        "3)}", String.class);
    Assert.assertEquals("DEBUG", result);

    result = eval.eval(variables,
      "${str:regExCapture(\"2015-01-18 22:31:51,813 DEBUG ZkClient - Received event: WatchedEvent state:Disconnected\"," +
        " \"(\\\\d{4}-\\\\d{2}-\\\\d{2}) (\\\\d{2}:\\\\d{2}:\\\\d{2},\\\\d{3}) ([^ ]*) ([^ ]*) - (.*)$\", " +
        "0)}", String.class);
    Assert.assertEquals("2015-01-18 22:31:51,813 DEBUG ZkClient - Received event: WatchedEvent state:Disconnected",
      result);
  }
}
