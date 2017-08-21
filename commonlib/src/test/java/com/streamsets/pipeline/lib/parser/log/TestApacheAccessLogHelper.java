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

public class TestApacheAccessLogHelper {

  private static final String COMMON_LOG_FORMAT = "%h %l %u [%t] \"%r\" %>s %b";
  private static final String COMMON_LOG_FORMAT_LOG_LINE =
    "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326";

  private static final String NCSA_COMBINED_LOG_FORMAT = "%h %l %u [%t] \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"";
  private static final String NCSA_COMBINED_LOG_FORMAT_LOG_LINE =
    "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326" +
    " \"http://www.example.com/start.html\" \"Mozilla/4.08 [en] (Win98; I ;Nav)\"";

  private static final String SIMPLE_FORMAT = "%H %m [%t] %U";
  private static final String SIMPLE_LOG_LINE = "HTTP/1.1 GET [12/Oct/2006:16:49:06 +0530] /index.php";

  private static final String RFC_822_DATE = "%{%a, %d %b %y %T %z}t";

  @Test
  public void testApacheFormatConversion() throws DataParserException {

    Assert.assertEquals(
      "^%{IPORHOST:remoteHost} %{USER:logName} %{USER:remoteUser} \\[(?<requestTime>%{HTTPDATE})\\] \"%{DATA:request}\" " +
        "%{NUMBER:status} (?:%{NUMBER:bytesSent}|-)",
      ApacheCustomLogHelper.translateApacheLayoutToGrok(COMMON_LOG_FORMAT));

    Assert.assertEquals(
      "^%{IPORHOST:remoteHost} %{USER:logName} %{USER:remoteUser} \\[(?<requestTime>%{HTTPDATE})\\] \"%{DATA:request}\" " +
        "%{NUMBER:status} (?:%{NUMBER:bytesSent}|-) \"%{DATA:referer}\" \"%{DATA:userAgent}\"",
      ApacheCustomLogHelper.translateApacheLayoutToGrok(NCSA_COMBINED_LOG_FORMAT));

    Assert.assertEquals(
      "^HTTP/%{NUMBER:httpversion} %{WORD:requestMethod} \\[(?<requestTime>%{HTTPDATE})\\] %{NOTSPACE:urlPath}",
      ApacheCustomLogHelper.translateApacheLayoutToGrok(SIMPLE_FORMAT));
  }

  @Test
  public void testCustomPatternConversion1() throws DataParserException {
    GrokDictionary grokDictionary = createGrokDictionary();

    Grok grok = grokDictionary.compileExpression(ApacheCustomLogHelper.translateApacheLayoutToGrok(COMMON_LOG_FORMAT));

    Map<String, String> namedGroupToValuesMap = grok.extractNamedGroups(COMMON_LOG_FORMAT_LOG_LINE);

    Assert.assertTrue(namedGroupToValuesMap.containsKey("remoteHost"));
    Assert.assertEquals("127.0.0.1", namedGroupToValuesMap.get("remoteHost"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("logName"));
    Assert.assertEquals("ss", namedGroupToValuesMap.get("logName"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("remoteUser"));
    Assert.assertEquals("h", namedGroupToValuesMap.get("remoteUser"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("requestTime"));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", namedGroupToValuesMap.get("requestTime"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("request"));
    Assert.assertEquals("GET /apache_pb.gif HTTP/1.0", namedGroupToValuesMap.get("request"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("status"));
    Assert.assertEquals("200", namedGroupToValuesMap.get("status"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("bytesSent"));
    Assert.assertEquals("2326", namedGroupToValuesMap.get("bytesSent"));
  }

  @Test
  public void testCustomPatternConversion2() throws DataParserException {
    GrokDictionary grokDictionary = createGrokDictionary();

    Grok grok = grokDictionary.compileExpression(
      ApacheCustomLogHelper.translateApacheLayoutToGrok(NCSA_COMBINED_LOG_FORMAT));

    Map<String, String> namedGroupToValuesMap = grok.extractNamedGroups(NCSA_COMBINED_LOG_FORMAT_LOG_LINE);

    Assert.assertTrue(namedGroupToValuesMap.containsKey("remoteHost"));
    Assert.assertEquals("127.0.0.1", namedGroupToValuesMap.get("remoteHost"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("logName"));
    Assert.assertEquals("ss", namedGroupToValuesMap.get("logName"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("remoteUser"));
    Assert.assertEquals("h", namedGroupToValuesMap.get("remoteUser"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("requestTime"));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", namedGroupToValuesMap.get("requestTime"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("request"));
    Assert.assertEquals("GET /apache_pb.gif HTTP/1.0", namedGroupToValuesMap.get("request"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("status"));
    Assert.assertEquals("200", namedGroupToValuesMap.get("status"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("bytesSent"));
    Assert.assertEquals("2326", namedGroupToValuesMap.get("bytesSent"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("referer"));
    Assert.assertEquals("http://www.example.com/start.html", namedGroupToValuesMap.get("referer"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("userAgent"));
    Assert.assertEquals("Mozilla/4.08 [en] (Win98; I ;Nav)", namedGroupToValuesMap.get("userAgent"));

  }

  @Test
  public void testCustomPatternConversion3() throws DataParserException {
    GrokDictionary grokDictionary = createGrokDictionary();

    Grok grok = grokDictionary.compileExpression(ApacheCustomLogHelper.translateApacheLayoutToGrok(SIMPLE_FORMAT));

    Map<String, String> namedGroupToValuesMap = grok.extractNamedGroups(SIMPLE_LOG_LINE);

    Assert.assertTrue(namedGroupToValuesMap.containsKey("httpversion"));
    Assert.assertEquals("1.1", namedGroupToValuesMap.get("httpversion"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("requestMethod"));
    Assert.assertEquals("GET", namedGroupToValuesMap.get("requestMethod"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("requestTime"));
    Assert.assertEquals("12/Oct/2006:16:49:06 +0530", namedGroupToValuesMap.get("requestTime"));

    Assert.assertTrue(namedGroupToValuesMap.containsKey("urlPath"));
    Assert.assertEquals("/index.php", namedGroupToValuesMap.get("urlPath"));

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
