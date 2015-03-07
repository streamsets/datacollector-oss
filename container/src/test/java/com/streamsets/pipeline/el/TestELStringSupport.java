/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.el.ELEvalException;
import org.junit.Assert;
import org.junit.Test;

public class TestELStringSupport {

  @Test
  public void testSubstring() throws Exception {
    ELEvaluator eval = new ELEvaluator("testSubstring", StringEL.class);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

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

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

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

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertEquals("StreamSets", eval.eval(variables,
      "${str:trim(\"   StreamSets  \")}", String.class));
  }

  @Test
  public void testToUpper() throws Exception {
    ELEvaluator eval = new ELEvaluator("testToUpper", StringEL.class);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertEquals("STREAMSETS", eval.eval(variables,
      "${str:toUpper(\"StreamSets\")}", String.class));
  }

  @Test
  public void testToLower() throws Exception {
    ELEvaluator eval = new ELEvaluator("testToLower", StringEL.class);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertEquals("streamsets inc", eval.eval(variables,
      "${str:toLower(\"StreamSets INC\")}", String.class));
  }

  @Test
  public void testReplace() throws Exception {
    ELEvaluator eval = new ELEvaluator("testReplace", StringEL.class);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertEquals("The.Streamsets.Inc", eval.eval(variables,
      "${str:replace(\"The Streamsets Inc\", ' ', '.')}", String.class));
  }

  @Test
  public void testReplaceAll() throws Exception {
    ELEvaluator eval = new ELEvaluator("testReplaceAll", StringEL.class);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertEquals("The Streamsets Company", eval.eval(variables,
      "${str:replaceAll(\"The Streamsets Inc\", \"Inc\", \"Company\")}", String.class));
  }

  @Test
  public void testContains() throws Exception {
    ELEvaluator eval = new ELEvaluator("testContains", StringEL.class);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertTrue(eval.eval(variables, "${str:contains(\"The Streamsets Inc\", \"Inc\")}", Boolean.class));
    Assert.assertFalse(eval.eval(variables, "${str:contains(\"The Streamsets Inc\", \"Incorporated\")}", Boolean.class));
  }

  @Test
  public void testStartsWith() throws Exception {
    ELEvaluator eval = new ELEvaluator("testStartsWith", StringEL.class);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertTrue(eval.eval(variables, "${str:startsWith(\"The Streamsets Inc\", \"The Streamsets\")}", Boolean.class));
    Assert.assertFalse(eval.eval(variables, "${str:startsWith(\"The Streamsets Inc\", \"Streamsets Inc\")}", Boolean.class));
  }

  @Test
  public void testEndsWith() throws Exception {
    ELEvaluator eval = new ELEvaluator("testEndsWith", StringEL.class);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertFalse(eval.eval(variables, "${str:endsWith(\"The Streamsets Inc\", \"The Streamsets\")}", Boolean.class));
    Assert.assertTrue(eval.eval(variables, "${str:endsWith(\"The Streamsets Inc\", \"Streamsets Inc\")}", Boolean.class));
  }

  @Test
  public void testTruncate() throws Exception {
    ELEvaluator eval = new ELEvaluator("testTruncate", StringEL.class);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertEquals("The StreamSets", eval.eval(variables,
      "${str:truncate(\"The StreamSets Inc\", 14)}", String.class));
  }

  @Test
  public void testRegExCapture() throws Exception {

    ELEvaluator eval = new ELEvaluator("testRegExCapture", StringEL.class);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

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
