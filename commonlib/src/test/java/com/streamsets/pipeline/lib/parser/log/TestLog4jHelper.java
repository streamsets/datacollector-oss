/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.dictionary.GrokDictionary;
import com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.util.Grok;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TestLog4jHelper {

  private static final String TTCC_LAYOUT = "%r [%t] %-5p %c %x - %m%n";
  private static final String TTCC_LAYOUT1 = "%-6r [%15.15t] %-5p %30.30c %x - %m%n";
  private static final String CUSTOM_LAYOUT = "%-5p [%t]: %m%n";
  private static final String DEFAULT_LOG4J = "%d{ISO8601} %-5p %c{1} - %m%n";

  @Test
  public void testLog4jConversion() throws DataParserException {

    Assert.assertEquals(
      "%{INT:relativetime} %{PROG:thread} %{LOGLEVEL:severity}(?:\\s*) %{JAVACLASS:category} %{WORD:ndc}? " +
      "- %{GREEDYDATA:message}\\r?\\n",
      Log4jHelper.translateLog4jLayoutToGrok(TTCC_LAYOUT));

    Assert.assertEquals(
      "%{INT:relativetime}(?:\\s*) (?:\\s*)%{PROG:thread} %{LOGLEVEL:severity}(?:\\s*) (?:\\s*)%{JAVACLASS:category} %{WORD:ndc}? " +
      "- %{GREEDYDATA:message}\\r?\\n",
      Log4jHelper.translateLog4jLayoutToGrok(TTCC_LAYOUT1));

    Assert.assertEquals(
      "(?<timestamp>%{TIMESTAMP_ISO8601}) %{LOGLEVEL:severity}(?:\\s*) %{JAVACLASS:category} " +
      "- %{GREEDYDATA:message}\\r?\\n",
      Log4jHelper.translateLog4jLayoutToGrok(DEFAULT_LOG4J));

    Assert.assertEquals("%{LOGLEVEL:severity}(?:\\s*) %{PROG:thread}: %{GREEDYDATA:message}\\r?\\n",
      Log4jHelper.translateLog4jLayoutToGrok(CUSTOM_LAYOUT));
  }

  @Test
  public void testCustomPatternConversion() throws DataParserException {
    GrokDictionary grokDictionary = new GrokDictionary();
    //Add grok patterns and Java patterns by default
    grokDictionary.addDictionary(getClass().getClassLoader().getResourceAsStream(Constants.GROK_PATTERNS_FILE_NAME));
    grokDictionary.addDictionary(getClass().getClassLoader().getResourceAsStream(
      Constants.GROK_JAVA_LOG_PATTERNS_FILE_NAME));
    grokDictionary.addDictionary(getClass().getClassLoader().getResourceAsStream(
      Constants.GROK_LOG4J_LOG_PATTERNS_FILE_NAME));
    grokDictionary.bind();

    //%-6r [%t] %-5p %30.30c - %m
    Grok grok = grokDictionary.compileExpression(Log4jHelper.translateLog4jLayoutToGrok(
      "%-6r [%15.15t] %-5p %30.30c - %m"));

    Map<String, String> namedGroupToValuesMap = grok.extractNamedGroups(
      "176 main INFO  org.apache.log4j.examples.Sort - Populating an array of 2 elements in reverse order."
    );

    Assert.assertEquals(5, namedGroupToValuesMap.size());
    Assert.assertTrue(namedGroupToValuesMap.containsKey("relativetime"));
    Assert.assertEquals("176", namedGroupToValuesMap.get("relativetime"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("thread"));
    Assert.assertEquals("main", namedGroupToValuesMap.get("thread"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("severity"));
    Assert.assertEquals("INFO", namedGroupToValuesMap.get("severity"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("category"));
    Assert.assertEquals("org.apache.log4j.examples.Sort", namedGroupToValuesMap.get("category"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("message"));
    Assert.assertEquals("Populating an array of 2 elements in reverse order.", namedGroupToValuesMap.get("message"));
  }
}
