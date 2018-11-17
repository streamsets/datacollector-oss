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
import com.streamsets.datacollector.definition.ELDefinitionExtractor;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringEL;
import com.streamsets.pipeline.lib.el.StringELConstants;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStringEL {
  private ELDefinitionExtractor elDefinitionExtractor = ConcreteELDefinitionExtractor.get();

  @Test
  public void testSubstring() throws Exception {
    ELEvaluator eval = new ELEvaluator("testSubstring", elDefinitionExtractor, StringEL.class);

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
    ELEvaluator eval = new ELEvaluator("testSubstringNegative", elDefinitionExtractor, StringEL.class);

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
    ELEvaluator eval = new ELEvaluator("testTrim", elDefinitionExtractor, StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertEquals("StreamSets", eval.eval(variables,
      "${str:trim(\"   StreamSets  \")}", String.class));
  }

  @Test
  public void testIndexOf() throws Exception {
    ELEvaluator eval = new ELEvaluator("testIndexOf", elDefinitionExtractor, StringEL.class);

    ELVariables variables = new ELVariables();
    Assert.assertEquals(new Integer(1), eval.eval(variables,
      "${str:indexOf(\"StreamSets\", \"t\")}", Integer.class));
  }

  @Test
  public void testToUpper() throws Exception {
    ELEvaluator eval = new ELEvaluator("testToUpper", elDefinitionExtractor, StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertEquals("STREAMSETS", eval.eval(variables,
      "${str:toUpper(\"StreamSets\")}", String.class));
  }

  @Test
  public void testToLower() throws Exception {
    ELEvaluator eval = new ELEvaluator("testToLower", elDefinitionExtractor, StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertEquals("streamsets inc", eval.eval(variables,
      "${str:toLower(\"StreamSets INC\")}", String.class));
  }

  @Test
  public void testReplace() throws Exception {
    ELEvaluator eval = new ELEvaluator("testReplace", elDefinitionExtractor, StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertEquals("The.Streamsets.Inc", eval.eval(variables,
      "${str:replace(\"The Streamsets Inc\", ' ', '.')}", String.class));

    // SDC-5165
    Assert.assertEquals("12345", eval.eval(variables, "${str:replace(\"_12345_\",\"_\",\"\")}", String.class));
  }

  @Test
  public void testReplaceAll() throws Exception {
    ELEvaluator eval = new ELEvaluator("testReplaceAll", elDefinitionExtractor, StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertEquals("The Streamsets Company", eval.eval(variables,
      "${str:replaceAll(\"The Streamsets Inc\", \"Inc\", \"Company\")}", String.class));
  }

  @Test
  public void testContains() throws Exception {
    ELEvaluator eval = new ELEvaluator("testContains", elDefinitionExtractor, StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertTrue(eval.eval(variables, "${str:contains(\"The Streamsets Inc\", \"Inc\")}", Boolean.class));
    Assert.assertFalse(eval.eval(variables, "${str:contains(\"The Streamsets Inc\", \"Incorporated\")}", Boolean.class));
  }

  @Test
  public void testStartsWith() throws Exception {
    ELEvaluator eval = new ELEvaluator("testStartsWith", elDefinitionExtractor, StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertTrue(eval.eval(variables, "${str:startsWith(\"The Streamsets Inc\", \"The Streamsets\")}", Boolean.class));
    Assert.assertFalse(eval.eval(variables, "${str:startsWith(\"The Streamsets Inc\", \"Streamsets Inc\")}", Boolean.class));
  }

  @Test
  public void testEndsWith() throws Exception {
    ELEvaluator eval = new ELEvaluator("testEndsWith", elDefinitionExtractor, StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertFalse(eval.eval(variables, "${str:endsWith(\"The Streamsets Inc\", \"The Streamsets\")}", Boolean.class));
    Assert.assertTrue(eval.eval(variables, "${str:endsWith(\"The Streamsets Inc\", \"Streamsets Inc\")}", Boolean.class));
  }

  @Test
  public void testTruncate() throws Exception {
    ELEvaluator eval = new ELEvaluator("testTruncate", elDefinitionExtractor, StringEL.class);

    ELVariables variables = new ELVariables();

    Assert.assertEquals("The StreamSets", eval.eval(variables,
      "${str:truncate(\"The StreamSets Inc\", 14)}", String.class));

    Assert.assertEquals("abc", eval.eval(variables,
        "${str:truncate(\"abcd\", 3)}", String.class));

    // endIndex is 0. it should return empty string
    Assert.assertEquals("", eval.eval(variables,
        "${str:truncate(\"abcd\", 0)}", String.class));

    // if original string is empty, it should return empty
    Assert.assertEquals("", eval.eval(variables,
        "${str:truncate(\"\", 3)}", String.class));

    // string is shorter than endIndex. It should return the original string without truncate
    Assert.assertEquals("abcd", eval.eval(variables,
        "${str:truncate(\"abcd\", 10)}", String.class));

    Assert.assertEquals("", eval.eval(variables,
        "${str:truncate(null, 10)}", String.class));

    Assert.assertEquals("", eval.eval(variables,
        "${str:truncate(\"\", 0)}", String.class));

    try {
      eval.eval(variables, "${str:truncate(\"abcd\", -4)}", String.class);
      Assert.fail("Should throw ELEvalException for negative endIndex");
    } catch (ELEvalException e) {
      //pass
    }
  }

  @Test
  public void testRegExCapture() throws Exception {

    ELEvaluator eval = new ELEvaluator("testRegExCapture", elDefinitionExtractor, StringEL.class);

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

  @Test
  public void testRegexCaptureMemoization() throws Exception {
    ELEvaluator eval = new ELEvaluator("testRegExCaptureMemoization", elDefinitionExtractor, StringEL.class);

    ELVariables variables = new ELVariables();

    Map<String, Pattern> memoizedRegex = new HashMap<>();
    variables.addContextVariable(StringELConstants.MEMOIZED, memoizedRegex);

    final String regex = ".*";

    eval.eval(variables, "${str:regExCapture(\"abcdef\", \"" + regex + "\", 0)}", String.class);
    Assert.assertEquals(1, memoizedRegex.size());
    Pattern compiledPattern = memoizedRegex.get(regex);

    eval.eval(variables, "${str:regExCapture(\"abcdef\", \"" + regex + "\", 0)}", String.class);
    // When executed again, make sure it still only has one pattern.
    Assert.assertEquals(1, memoizedRegex.size());
    // Same regex instance (no new regex was compiled
    Assert.assertEquals(compiledPattern, memoizedRegex.get(regex));
    // Regex pattern was the one expected
    Assert.assertEquals(Pattern.compile(".*").pattern(), memoizedRegex.get(regex).pattern());
  }

  @Test
  public void testConcat() throws Exception {
    ELEvaluator eval = new ELEvaluator("testConcat", elDefinitionExtractor, StringEL.class);
    ELVariables variables = new ELVariables();
    String result = eval.eval(variables, "${str:concat(\"abc\", \"def\")}", String.class);
    Assert.assertEquals("abcdef", result);
    result = eval.eval(variables, "${str:concat(\"\", \"def\")}", String.class);
    Assert.assertEquals("def", result);
    result = eval.eval(variables, "${str:concat(\"abc\", \"\")}", String.class);
    Assert.assertEquals("abc", result);
    result = eval.eval(variables, "${str:concat(\"\", \"\")}", String.class);
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testMatches() throws Exception {
    ELEvaluator eval = new ELEvaluator("testMatches", elDefinitionExtractor, StringEL.class);
    ELVariables variables = new ELVariables();

    Assert.assertTrue(eval.eval(variables, "${str:matches(\"abc\", \"[a-z]+\")}", Boolean.class));
    Assert.assertTrue(eval.eval(variables, "${str:matches(\"abc123\", \"[a-z]+[0-9]+\")}", Boolean.class));
    Assert.assertTrue(eval.eval(variables, "${str:matches(\"abc123\", \".*\")}", Boolean.class));
    Assert.assertTrue(eval.eval(variables, "${str:matches(\"  bc  cd\", \"[a-z ]+\")}", Boolean.class));
    Assert.assertTrue(eval.eval(variables,
        "${str:matches(\"vcpip-hdvrjkdfkjd\", \"^(vc[rkpm]ip-hdvr.*)\")}", Boolean.class));

    Assert.assertFalse(eval.eval(variables, "${str:matches(\"Abc\", \"[a-z]+\")}", Boolean.class));
    Assert.assertFalse(eval.eval(variables, "${str:matches(\"abc\n123\", \".*\")}", Boolean.class));
  }

  @Test
  public void testStringLength() throws Exception {
    ELEvaluator eval = new ELEvaluator("testStringLength", elDefinitionExtractor, StringEL.class);
    ELVariables variables = new ELVariables();
    Assert.assertTrue(eval.eval(variables, "${str:length(\"abc\")}", Integer.class) == 3);
    Assert.assertTrue(eval.eval(variables, "${str:length(\"\")}", Integer.class) == 0);
    Assert.assertTrue(eval.eval(variables, "${str:length(str:concat(\"abc\",\"def\"))}", Integer.class) == 6);
    Assert.assertTrue(eval.eval(variables, "${str:length(str:trim(\" abc \"))}", Integer.class) == 3);
    Assert.assertTrue(eval.eval(variables, "${str:length(str:substring(\"abcdef\", 0, 3))}", Integer.class) == 3);
  }

  @Test
  public void testStringLastIndexOf() throws Exception {
    ELEvaluator eval = new ELEvaluator("testStringLastIndexOf", elDefinitionExtractor, StringEL.class);
    ELVariables variables = new ELVariables();
    Assert.assertTrue(eval.eval(variables, "${str:lastIndexOf('aabac', 'a')}", Integer.class) == 3);
    Assert.assertTrue(eval.eval(variables, "${str:lastIndexOf('abcxyzabcxyz', 'xyz')}", Integer.class) == 9);
    Assert.assertTrue(eval.eval(variables, "${str:lastIndexOf('abcd', 'e')}", Integer.class) == -1);
  }

  @Test
  public void testUrlEncode() throws Exception {
    ELEvaluator eval = new ELEvaluator("testUrlEncode", elDefinitionExtractor, StringEL.class);
    ELVariables variables = new ELVariables();
    Assert.assertEquals("", eval.eval(variables, "${str:urlEncode(\"\", \"UTF8\")}", String.class));
    Assert.assertEquals("Ahoj+tady+medusa", eval.eval(variables, "${str:urlEncode(\"Ahoj tady medusa\", \"UTF8\")}", String.class));
  }

  @Test
  public void testUrlDecode() throws Exception {
    ELEvaluator eval = new ELEvaluator("testUrlDecode", elDefinitionExtractor, StringEL.class);
    ELVariables variables = new ELVariables();
    Assert.assertEquals("", eval.eval(variables, "${str:urlDecode(\"\", \"UTF8\")}", String.class));
    Assert.assertEquals("Ahoj tady medusa", eval.eval(variables, "${str:urlDecode(\"Ahoj+tady+medusa\", \"UTF8\")}", String.class));
  }

  @Test
  public void testEscapeXml10() throws Exception {
    ELEvaluator eval = new ELEvaluator("testXmlEscape10", elDefinitionExtractor, StringEL.class);
    ELVariables variables = new ELVariables();
    Assert.assertEquals("", eval.eval(variables, "${str:escapeXML10(\"\")}", String.class));
    Assert.assertEquals(
        "&lt; abc &amp; def &gt; ", // the 0001 control char is removed because not compatible with XML 1.0
        eval.eval(variables, "${str:escapeXML10(\"< abc & def > \u0001\")}", String.class)
    );
  }

  @Test
  public void testEscapeXml11() throws Exception {
    ELEvaluator eval = new ELEvaluator("testXmlEscape10", elDefinitionExtractor, StringEL.class);
    ELVariables variables = new ELVariables();
    Assert.assertEquals("", eval.eval(variables, "${str:escapeXML11(\"\")}", String.class));
    Assert.assertEquals(
        "&lt; abc &amp; def &gt; &#1;",
        eval.eval(variables, "${str:escapeXML11(\"< abc & def > \u0001\")}", String.class)
    );
  }

  @Test
  public void testUnescapeXml() throws Exception {
    ELEvaluator eval = new ELEvaluator("testXmlEscape10", elDefinitionExtractor, StringEL.class);
    ELVariables variables = new ELVariables();
    Assert.assertEquals("", eval.eval(variables, "${str:unescapeXML(\"\")}", String.class));
    Assert.assertEquals("< abc & def > \u0001",
        eval.eval(
            variables,
            "${str:unescapeXML(\"&lt; abc &amp; def &gt; &#1;\")}",
            String.class
        )
    );
  }

  @Test
  public void testUnescapeJava() throws Exception {
    ELEvaluator eval = new ELEvaluator("testUnescapeJava", elDefinitionExtractor, StringEL.class);
    ELVariables variables = new ELVariables();
    Assert.assertEquals("\n", eval.eval(variables, "${str:unescapeJava(\"\\\\n\")}", String.class));
  }

  @Test
  public void testListJoiner() throws Exception {
    ELEvaluator eval = new ELEvaluator("testJoiner", elDefinitionExtractor, StringEL.class, RecordEL.class);
    ELVariables variables = new ELVariables();
    Record record = mock(Record.class);
    List<Field> listField = IntStream.range(1, 5).mapToObj(Field::create).collect(Collectors.toList());
    listField.set(2, Field.create(Field.Type.INTEGER, null));
    when(record.get("/list")).thenReturn(Field.create(listField));
    RecordEL.setRecordInContext(variables, record);
    Assert.assertEquals("1,2,null,4", eval.eval(variables, "${list:join(record:value('/list'), ',')}", String.class));
    Assert.assertEquals("", eval.eval(variables, "${list:join(record:value('/not-there'), ',')}", String.class));
  }

  @Test
  public void testListJoinerSkipNulls() throws Exception {
    ELEvaluator eval = new ELEvaluator("testJoiner", elDefinitionExtractor, StringEL.class, RecordEL.class);
    ELVariables variables = new ELVariables();
    Record record = mock(Record.class);
    List<Field> listField = IntStream.range(1, 5).mapToObj(Field::create).collect(Collectors.toList());
    listField.set(2, Field.create(Field.Type.INTEGER, null));
    when(record.get("/list")).thenReturn(Field.create(listField));
    RecordEL.setRecordInContext(variables, record);
    Assert.assertEquals("1,2,4", eval.eval(variables, "${list:joinSkipNulls(record:value('/list'), ',')}", String.class));
    Assert.assertEquals("", eval.eval(variables, "${list:joinSkipNulls(record:value('/not-there'), ',')}", String.class));
  }

  @Test
  public void testMapJoiner() throws Exception {
    ELEvaluator eval = new ELEvaluator("testJoiner", elDefinitionExtractor, StringEL.class, RecordEL.class);
    ELVariables variables = new ELVariables();
    Record record = mock(Record.class);
    Map<String, Field> mapField = ImmutableMap.of(
        "a",
        Field.create(1),
        "b",
        Field.create(Field.Type.INTEGER, null),
        "c",
        Field.create("xyz")
    );
    when(record.get("/map")).thenReturn(Field.create(mapField));
    RecordEL.setRecordInContext(variables, record);
    Assert.assertEquals(
        "a=1,b=null,c=xyz",
        eval.eval(variables, "${map:join(record:value('/map'), ',', '=')}", String.class)
    );
    Assert.assertEquals("", eval.eval(variables, "${map:join(record:value('/not-there'), ',', '=')}", String.class)
    );
  }

  @Test
  public void testUuid() throws Exception {
    final String UUID_FORMAT = "^[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$";
    ELEvaluator eval = new ELEvaluator("testUuid", elDefinitionExtractor, StringEL.class);
    ELVariables variables = new ELVariables();

    String uuid = eval.eval(variables, "${uuid:uuid()}", String.class);
    Assert.assertNotNull(uuid);
    Assert.assertTrue("UUID does not match expected format: " + uuid, uuid.matches(UUID_FORMAT));

    String uuid2 = eval.eval(variables, "${uuid:uuid()}", String.class);
    Assert.assertNotNull(uuid);
    Assert.assertTrue("UUID does not match expected format: " + uuid, uuid.matches(UUID_FORMAT));

    Assert.assertNotEquals(uuid, uuid2);
  }

  @Test
  public void testEmptyStringFunctions() throws Exception {
    ELEvaluator eval = new ELEvaluator("testEmptyStringFunctions", elDefinitionExtractor, StringEL.class);
    ELVariables vars = new ELVariables();

    Assert.assertTrue(eval.eval(vars, "${str:isNullOrEmpty('')}", Boolean.class));
    Assert.assertTrue(eval.eval(vars, "${str:isNullOrEmpty(NULL)}", Boolean.class));
  }

  @Test
  public void testKVSplitter() throws Exception {
    ELEvaluator eval = new ELEvaluator("kvSplitter", elDefinitionExtractor, StringEL.class);
    ELVariables vars = new ELVariables();

    Map<String, Field> result = eval.eval(vars, "${str:splitKV('key1=val1&key2=val2&key3=val3', '&', '=')}", Map.class);

    Assert.assertEquals(3, result.size());
    for (int i = 1; i <= 3; i++) {
      Assert.assertTrue(result.containsKey("key" + i));
      Assert.assertEquals("val" + i, result.get("key" + i).getValueAsString());
    }
  }

  @Test
  public void testKVSplitterWithSeparatorInValue() throws Exception {
    ELEvaluator eval = new ELEvaluator("kvSplitter", elDefinitionExtractor, StringEL.class);
    ELVariables vars = new ELVariables();

    Map<String, Field> result = eval.eval(vars, "${str:splitKV('key1=val1&key2=val=2', '&', '=')}", Map.class);

    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.containsKey("key1"));
    Assert.assertEquals("val1", result.get("key1").getValueAsString());
    Assert.assertTrue(result.containsKey("key2"));
    Assert.assertEquals("val=2", result.get("key2").getValueAsString());
  }

  @Test
  public void testSplitter() throws Exception {
    ELEvaluator eval = new ELEvaluator("splitter", elDefinitionExtractor, StringEL.class);
    ELVariables vars = new ELVariables();

    List<Field> result = eval.eval(vars, "${str:split('a, b, c', ',')}", List.class);
    Assert.assertEquals(3, result.size());
    Assert.assertEquals("a", result.get(0).getValueAsString());
    Assert.assertEquals("b", result.get(1).getValueAsString());
    Assert.assertEquals("c", result.get(2).getValueAsString());
  }

}
