/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import org.junit.Assert;
import org.junit.Test;

import javax.servlet.jsp.el.ELException;

public class TestELStringSupport {

  @Test
  public void testSubstring() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELStringSupport.registerStringFunctions(eval);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertEquals("StreamSets", eval.eval(variables,
      "${str:substring(\"The StreamSets Inc\", 4, 14)}"));

    //End index greater than length, return beginIndex to end of string
    Assert.assertEquals("StreamSets Inc", eval.eval(variables,
      "${str:substring(\"The StreamSets Inc\", 4, 50)}"));

    //Begin Index > length, return ""
    Assert.assertEquals("", eval.eval(variables,
      "${str:substring(\"The StreamSets Inc\", 50, 60)}"));
  }

  @Test
  public void testSubstringNegative() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELStringSupport.registerStringFunctions(eval);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    try {
      eval.eval(variables, "${str:substring(\"The StreamSets Inc\", -1, 14)}");
      Assert.fail("ELException expected as the begin index is negative");
    } catch (ELException e) {

    }

    try {
      eval.eval(variables, "${str:substring(\"The StreamSets Inc\", 0, -3)}");
      Assert.fail("ELException expected as the end index is negative");
    } catch (ELException e) {

    }

    //Input is empty, return empty
    Assert.assertEquals("", eval.eval(variables, "${str:substring(\"\", 0, 5)}"));
  }

  @Test
  public void testTrim() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELStringSupport.registerStringFunctions(eval);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertEquals("StreamSets", eval.eval(variables,
      "${str:trim(\"   StreamSets  \")}"));
  }

  @Test
  public void testToUpper() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELStringSupport.registerStringFunctions(eval);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertEquals("STREAMSETS", eval.eval(variables,
      "${str:toUpper(\"StreamSets\")}"));
  }

  @Test
  public void testToLower() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELStringSupport.registerStringFunctions(eval);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertEquals("streamsets inc", eval.eval(variables,
      "${str:toLower(\"StreamSets INC\")}"));
  }

  @Test
  public void testReplace() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELStringSupport.registerStringFunctions(eval);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertEquals("The.Streamsets.Inc", eval.eval(variables,
      "${str:replace(\"The Streamsets Inc\", ' ', '.')}"));
  }

  @Test
  public void testReplaceAll() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELStringSupport.registerStringFunctions(eval);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertEquals("The Streamsets Company", eval.eval(variables,
      "${str:replaceAll(\"The Streamsets Inc\", \"Inc\", \"Company\")}"));
  }

  @Test
  public void testContains() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELStringSupport.registerStringFunctions(eval);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertTrue((boolean) eval.eval(variables, "${str:contains(\"The Streamsets Inc\", \"Inc\")}"));
    Assert.assertFalse((boolean) eval.eval(variables, "${str:contains(\"The Streamsets Inc\", \"Incorporated\")}"));
  }

  @Test
  public void testStartsWith() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELStringSupport.registerStringFunctions(eval);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertTrue((boolean) eval.eval(variables, "${str:startsWith(\"The Streamsets Inc\", \"The Streamsets\")}"));
    Assert.assertFalse((boolean) eval.eval(variables, "${str:startsWith(\"The Streamsets Inc\", \"Streamsets Inc\")}"));
  }

  @Test
  public void testEndsWith() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELStringSupport.registerStringFunctions(eval);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertFalse((boolean) eval.eval(variables, "${str:endsWith(\"The Streamsets Inc\", \"The Streamsets\")}"));
    Assert.assertTrue((boolean) eval.eval(variables, "${str:endsWith(\"The Streamsets Inc\", \"Streamsets Inc\")}"));
  }

  @Test
  public void testTruncate() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELStringSupport.registerStringFunctions(eval);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertEquals("The StreamSets", eval.eval(variables,
      "${str:truncate(\"The StreamSets Inc\", 14)}"));
  }

  @Test
  public void testRegExCapture() throws Exception {

    ELEvaluator eval = new ELEvaluator();
    ELStringSupport.registerStringFunctions(eval);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    String result = (String)eval.eval(variables,
      "${str:regExCapture(\"2015-01-18 22:31:51,813 DEBUG ZkClient - Received event: WatchedEvent state:Disconnected\"," +
        " \"(\\\\d{4}-\\\\d{2}-\\\\d{2}) (\\\\d{2}:\\\\d{2}:\\\\d{2},\\\\d{3}) ([^ ]*) ([^ ]*) - (.*)$\", " +
        "1)}");
    Assert.assertEquals("2015-01-18", result);

    result = (String)eval.eval(variables,
      "${str:regExCapture(\"2015-01-18 22:31:51,813 DEBUG ZkClient - Received event: WatchedEvent state:Disconnected\"," +
        " \"(\\\\d{4}-\\\\d{2}-\\\\d{2}) (\\\\d{2}:\\\\d{2}:\\\\d{2},\\\\d{3}) ([^ ]*) ([^ ]*) - (.*)$\", " +
        "2)}");
    Assert.assertEquals("22:31:51,813", result);

    result = (String)eval.eval(variables,
      "${str:regExCapture(\"2015-01-18 22:31:51,813 DEBUG ZkClient - Received event: WatchedEvent state:Disconnected\"," +
        " \"(\\\\d{4}-\\\\d{2}-\\\\d{2}) (\\\\d{2}:\\\\d{2}:\\\\d{2},\\\\d{3}) ([^ ]*) ([^ ]*) - (.*)$\", " +
        "3)}");
    Assert.assertEquals("DEBUG", result);

    result = (String)eval.eval(variables,
      "${str:regExCapture(\"2015-01-18 22:31:51,813 DEBUG ZkClient - Received event: WatchedEvent state:Disconnected\"," +
        " \"(\\\\d{4}-\\\\d{2}-\\\\d{2}) (\\\\d{2}:\\\\d{2}:\\\\d{2},\\\\d{3}) ([^ ]*) ([^ ]*) - (.*)$\", " +
        "0)}");
    Assert.assertEquals("2015-01-18 22:31:51,813 DEBUG ZkClient - Received event: WatchedEvent state:Disconnected",
      result);
  }
}
