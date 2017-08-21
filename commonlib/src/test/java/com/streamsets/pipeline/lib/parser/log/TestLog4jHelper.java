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
      "^%{INT:relativetime} \\[%{DATA:thread}\\] %{LOGLEVEL:severity}(?:\\s*) %{JAVACLASS:category} %{DATA:ndc}? " +
      "- %{GREEDYDATA:message}",
      Log4jHelper.translateLog4jLayoutToGrok(TTCC_LAYOUT));

    Assert.assertEquals(
      "^%{INT:relativetime}(?:\\s*) \\[(?:\\s*)%{DATA:thread}\\] %{LOGLEVEL:severity}(?:\\s*) (?:\\s*)%{JAVACLASS:category} %{DATA:ndc}? " +
      "- %{GREEDYDATA:message}",
      Log4jHelper.translateLog4jLayoutToGrok(TTCC_LAYOUT1));

    Assert.assertEquals(
      "^(?<timestamp>%{TIMESTAMP_ISO8601}) %{LOGLEVEL:severity}(?:\\s*) %{JAVACLASS:category} " +
      "- %{GREEDYDATA:message}",
      Log4jHelper.translateLog4jLayoutToGrok(DEFAULT_LOG4J));

    Assert.assertEquals("^%{LOGLEVEL:severity}(?:\\s*) \\[%{DATA:thread}\\]: %{GREEDYDATA:message}",
      Log4jHelper.translateLog4jLayoutToGrok(CUSTOM_LAYOUT));
  }

  @Test
  public void testCustomPatternConversion1() throws DataParserException {
    GrokDictionary grokDictionary = createGrokDictionary();

    //%-6r [%t] %-5p %30.30c - %m
    Grok grok = grokDictionary.compileExpression(Log4jHelper.translateLog4jLayoutToGrok(
      "%-6r [%15.15t] %-5p %30.30c - %m%n"));

    Map<String, String> namedGroupToValuesMap = grok.extractNamedGroups(
      "176 [main] INFO  org.apache.log4j.examples.Sort - Populating an array of 2 elements in reverse order."
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

  @Test
  public void testCustomPatternConversion2() throws DataParserException {
    GrokDictionary grokDictionary = createGrokDictionary();

    Grok grok = grokDictionary.compileExpression(Log4jHelper.translateLog4jLayoutToGrok(
      "%p %d %c %M - %m%n"));

    Map<String, String> namedGroupToValuesMap = grok.extractNamedGroups(
      "INFO 2012-11-02 22:46:43,896 MyClass foo - this is a log message"
    );

    Assert.assertEquals(5, namedGroupToValuesMap.size());
    Assert.assertTrue(namedGroupToValuesMap.containsKey("severity"));
    Assert.assertEquals("INFO", namedGroupToValuesMap.get("severity"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("timestamp"));
    Assert.assertEquals("2012-11-02 22:46:43,896", namedGroupToValuesMap.get("timestamp"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("category"));
    Assert.assertEquals("MyClass", namedGroupToValuesMap.get("category"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("method"));
    Assert.assertEquals("foo", namedGroupToValuesMap.get("method"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("message"));
    Assert.assertEquals("this is a log message", namedGroupToValuesMap.get("message"));

  }

  @Test
  public void testCustomPatternConversion3() throws DataParserException {
    GrokDictionary grokDictionary = createGrokDictionary();
    Grok grok = grokDictionary.compileExpression(Log4jHelper.translateLog4jLayoutToGrok(
      "[%p] %d %c %M - %m%n"));

    Map<String, String> namedGroupToValuesMap = grok.extractNamedGroups(
      "[INFO] 2012-11-02 22:46:43,896 MyClass foo - this is a log message"
    );

    Assert.assertEquals(5, namedGroupToValuesMap.size());
    Assert.assertTrue(namedGroupToValuesMap.containsKey("severity"));
    Assert.assertEquals("INFO", namedGroupToValuesMap.get("severity"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("timestamp"));
    Assert.assertEquals("2012-11-02 22:46:43,896", namedGroupToValuesMap.get("timestamp"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("category"));
    Assert.assertEquals("MyClass", namedGroupToValuesMap.get("category"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("method"));
    Assert.assertEquals("foo", namedGroupToValuesMap.get("method"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("message"));
    Assert.assertEquals("this is a log message", namedGroupToValuesMap.get("message"));
  }

  @Test
  public void testCustomPatternConversion4() throws DataParserException {
    GrokDictionary grokDictionary = createGrokDictionary();
    Grok grok = grokDictionary.compileExpression(Log4jHelper.translateLog4jLayoutToGrok(
      "[%p] %d{ABSOLUTE} %c %M - %m%n"));

    Map<String, String> namedGroupToValuesMap = grok.extractNamedGroups(
      "[INFO] 23:17:07,097 MyClass foo - this is a log message"
    );

    Assert.assertEquals(5, namedGroupToValuesMap.size());
    Assert.assertTrue(namedGroupToValuesMap.containsKey("severity"));
    Assert.assertEquals("INFO", namedGroupToValuesMap.get("severity"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("timestamp"));
    Assert.assertEquals("23:17:07,097", namedGroupToValuesMap.get("timestamp"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("category"));
    Assert.assertEquals("MyClass", namedGroupToValuesMap.get("category"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("method"));
    Assert.assertEquals("foo", namedGroupToValuesMap.get("method"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("message"));
    Assert.assertEquals("this is a log message", namedGroupToValuesMap.get("message"));
  }

  @Test
  public void testCustomPatternConversion5() throws DataParserException {
    GrokDictionary grokDictionary = createGrokDictionary();
    Grok grok = grokDictionary.compileExpression(Log4jHelper.translateLog4jLayoutToGrok(
      "[%p] %d{DATE} %c %M - %m%n"));

    Map<String, String> namedGroupToValuesMap = grok.extractNamedGroups(
      "[INFO] 02 Nov 2012 23:18:19,547 MyClass foo - this is a log message"
    );

    Assert.assertEquals(5, namedGroupToValuesMap.size());
    Assert.assertTrue(namedGroupToValuesMap.containsKey("severity"));
    Assert.assertEquals("INFO", namedGroupToValuesMap.get("severity"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("timestamp"));
    Assert.assertEquals("02 Nov 2012 23:18:19,547", namedGroupToValuesMap.get("timestamp"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("category"));
    Assert.assertEquals("MyClass", namedGroupToValuesMap.get("category"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("method"));
    Assert.assertEquals("foo", namedGroupToValuesMap.get("method"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("message"));
    Assert.assertEquals("this is a log message", namedGroupToValuesMap.get("message"));
  }

  @Test
  public void testCustomPatternConversion6() throws DataParserException {
    GrokDictionary grokDictionary = createGrokDictionary();
    Grok grok = grokDictionary.compileExpression(Log4jHelper.translateLog4jLayoutToGrok(
      "[%p] %d{MM-dd-yyyy HH:mm:ss,SSS} %c %M - %m%n"));

    Map<String, String> namedGroupToValuesMap = grok.extractNamedGroups(
      "[INFO] 11-02-2012 23:09:38,304 MyClass foo - this is a log message"
    );

    Assert.assertEquals(5, namedGroupToValuesMap.size());
    Assert.assertTrue(namedGroupToValuesMap.containsKey("severity"));
    Assert.assertEquals("INFO", namedGroupToValuesMap.get("severity"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("timestamp"));
    Assert.assertEquals("11-02-2012 23:09:38,304", namedGroupToValuesMap.get("timestamp"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("category"));
    Assert.assertEquals("MyClass", namedGroupToValuesMap.get("category"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("method"));
    Assert.assertEquals("foo", namedGroupToValuesMap.get("method"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("message"));
    Assert.assertEquals("this is a log message", namedGroupToValuesMap.get("message"));
  }

  @Test
  public void testLogWithMDC() throws DataParserException {
    GrokDictionary grokDictionary = createGrokDictionary();
    String s = Log4jHelper.translateLog4jLayoutToGrok(
      "%d{ISO8601} [user:%X{s-user}] [pipeline:%X{s-entity}] [thread:%t]  %-5p %c{1} - %m%n");
    Grok grok = grokDictionary.compileExpression(s);

    Map<String, String> namedGroupToValuesMap = grok.extractNamedGroups(
      "2015-08-07 06:14:18,029 [user:*admin] [pipeline:hari_test_geoIP_proc] [thread:preview-pool-1-thread-1]  DEBUG DirectorySpooler - Last file found 'common_log_format.log' on startup"
    );

    Assert.assertEquals(7, namedGroupToValuesMap.size());
    Assert.assertTrue(namedGroupToValuesMap.containsKey("timestamp"));
    Assert.assertEquals("2015-08-07 06:14:18,029", namedGroupToValuesMap.get("timestamp"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("s-user"));
    Assert.assertEquals("*admin", namedGroupToValuesMap.get("s-user"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("s-entity"));
    Assert.assertEquals("hari_test_geoIP_proc", namedGroupToValuesMap.get("s-entity"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("thread"));
    Assert.assertEquals("preview-pool-1-thread-1", namedGroupToValuesMap.get("thread"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("severity"));
    Assert.assertEquals("DEBUG", namedGroupToValuesMap.get("severity"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("category"));
    Assert.assertEquals("DirectorySpooler", namedGroupToValuesMap.get("category"));
    Assert.assertTrue(namedGroupToValuesMap.containsKey("message"));
    Assert.assertEquals("Last file found 'common_log_format.log' on startup", namedGroupToValuesMap.get("message"));
  }

  @Test
  public void testLogWithNDC() throws DataParserException {
    GrokDictionary grokDictionary = createGrokDictionary();
    String s = Log4jHelper.translateLog4jLayoutToGrok("%d{ISO8601} %-5p [%t %x] [%c{1}] – %m%n");

    Grok grok = grokDictionary.compileExpression(s);

    Map<String, String> namedGroupToValuesMap = grok.extractNamedGroups(
      "2009-12-02 11:00:50,806 INFO  [15554445@qtp-10481519-2 PUT /member/03003522/foostrategy] [FooController] – Updating FooStrategy[memberNumber=1234");

    Assert.assertEquals(6, namedGroupToValuesMap.size());

    Assert.assertTrue(namedGroupToValuesMap.containsKey("timestamp"));
    Assert.assertEquals("2009-12-02 11:00:50,806", namedGroupToValuesMap.get("timestamp"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("severity"));
    Assert.assertEquals("INFO", namedGroupToValuesMap.get("severity"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("thread"));
    Assert.assertEquals("15554445@qtp-10481519-2", namedGroupToValuesMap.get("thread"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("ndc"));
    Assert.assertEquals("PUT /member/03003522/foostrategy", namedGroupToValuesMap.get("ndc"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("category"));
    Assert.assertEquals("FooController", namedGroupToValuesMap.get("category"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("message"));
    Assert.assertEquals("Updating FooStrategy[memberNumber=1234", namedGroupToValuesMap.get("message"));
  }

  private GrokDictionary createGrokDictionary() {
    GrokDictionary grokDictionary = new GrokDictionary();
    //Add grok patterns and Java patterns by default
    grokDictionary.addDictionary(getClass().getClassLoader().getResourceAsStream(Constants.GROK_PATTERNS_FILE_NAME));
    grokDictionary.addDictionary(getClass().getClassLoader().getResourceAsStream(
      Constants.GROK_JAVA_LOG_PATTERNS_FILE_NAME));
    grokDictionary.addDictionary(getClass().getClassLoader().getResourceAsStream(
      Constants.GROK_LOG4J_LOG_PATTERNS_FILE_NAME));
    grokDictionary.bind();
    return grokDictionary;
  }
}
