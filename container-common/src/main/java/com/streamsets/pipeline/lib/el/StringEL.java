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
package com.streamsets.pipeline.lib.el;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StringEL {
  public static final String MEMOIZED = "memoized";
  private static final Logger LOG = LoggerFactory.getLogger(StringEL.class);

  private StringEL() {
  }

  @ElFunction(
    prefix = "str",
    name = "substring",
    description = "Returns a new string that is a substring of this string. " +
    "The substring begins at the specified beginIndex and extends to the character at index endIndex-1. " +
    "Thus the length of the substring is endIndex-beginIndex")
  public static String substring(
    @ElParam("string") String string,
    @ElParam("beginIndex") int beginIndex,
    @ElParam("endIndex") int endIndex) {
    Utils.checkArgument(beginIndex >= 0, "Argument beginIndex should be 0 or greater");
    Utils.checkArgument(endIndex >= 0, "Argument endIndex should be 0 or greater");

    if(string == null || string.isEmpty()) {
      return string;
    }
    int length = string.length();
    if(beginIndex > length) {
      return "";
    }
    if(endIndex > string.length()) {
      endIndex = string.length();
    }
    return string.substring(beginIndex, endIndex);
  }

  @ElFunction(
    prefix = "str",
    name = "indexOf",
    description = "Returns the index within the string of the first occurrence of the specified substring."
  )
  public static int indexOf(
    @ElParam("string") String string, @ElParam("substring") String substring) {
    return string.indexOf(substring);
  }


  @ElFunction(
    prefix = "str",
    name = "trim",
    description = "Removes leading and trailing whitespaces")
  public static String trim(
    @ElParam("string") String string) {
    return string.trim();
  }

  @ElFunction(
      prefix = "str",
      name = "isNullOrEmpty",
      description = "Returns true if the string is null or empty"
  )
  public static boolean isNullOrEmpty(@ElParam("string") String string) {
    return Strings.isNullOrEmpty(string);
  }

  @ElFunction(
    prefix = "str",
    name = "toUpper",
    description = "Converts all of the characters in the argument string to uppercase")
  public static String toUpper(
    @ElParam("string") String string) {
    return string.toUpperCase();
  }

  @ElFunction(
    prefix = "str",
    name = "toLower",
    description = "Converts all of the characters in the argument string to lowercase")
  public static String toLower(
    @ElParam("string") String string) {
    return string.toLowerCase();
  }

  @ElFunction(
    prefix = "str",
    name = "replace",
    description = "Returns a new string resulting from replacing all occurrences of oldString in this string with newString")
  public static String replace(
    @ElParam("string") String string,
    @ElParam("oldString") String oldString,
    @ElParam("newString") String newString) {
    return string.replace(oldString, newString);
  }

  @ElFunction(
    prefix = "str",
    name = "replaceAll",
    description = "Replaces each substring of this string that matches the given regEx with the given replacement")
  public static String replaceAll(
    @ElParam("string") String string,
    @ElParam("regEx") String regEx,
    @ElParam("replacement") String replacement) {
    return string.replaceAll(regEx, replacement);
  }

  @ElFunction(
    prefix = "str",
    name = "truncate",
    description = "Truncates the argument string to the given index")
  public static String truncate(
    @ElParam("string") String string,
    @ElParam("endIndex") int endIndex) {
    if (string == null){
      return "";
    }
    if (endIndex < 0) {
      throw new IllegalArgumentException(String.format("Unable to truncate '%s' at index %s", string, endIndex));
    }
    if (endIndex > string.length()){
      LOG.warn("Attempted to truncate '{}' at index {}. Returning '{}'", string, endIndex, string);
      return string;
    }
    return string.substring(0, endIndex);
  }

  @SuppressWarnings("unchecked")
  @ElFunction(
    prefix = "str",
    name = "regExCapture",
    description = "Captures the string that matches the argument regular expression and the group")
  public static String regExCapture(
    @ElParam("string") String string,
    @ElParam("regEx") String regEx,
    @ElParam("groupNumber") int groupNumber) {
    Utils.checkArgument(regEx != null, "Argument regEx for str:regExCapture() cannot be null.");
    if (string != null) {
      Map<String, Pattern> patterns = (Map<String, Pattern>) ELEval.getVariablesInScope().getContextVariable(MEMOIZED);
      Matcher matcher = getPattern(patterns, regEx).matcher(string);
      if (matcher.find()) {
        return matcher.group(groupNumber);
      }
    }
    return null;
  }

  private static Pattern getPattern(Map<String, Pattern> patterns, String regEx) {
    if (patterns != null && patterns.containsKey(regEx)) {
      return patterns.get(regEx);
    } else {
      Pattern pattern = Pattern.compile(regEx);
      if (patterns != null) {
        patterns.put(regEx, pattern);
      }
      return pattern;
    }
  }

  @ElFunction(
    prefix = "str",
    name = "contains",
    description = "Indicates whether the argument string contains the specified substring.")
  public static boolean contains(
    @ElParam("string") String string,
    @ElParam("substring") String substring) {
    return string.contains(substring);
  }

  @ElFunction(
    prefix = "str",
    name = "startsWith",
    description = "Indicates whether the argument string starts with the specified prefix")
  public static boolean startsWith(
    @ElParam("string") String string,
    @ElParam("prefix") String prefix) {
    return string.startsWith(prefix);
  }

  @ElFunction(
    prefix = "str",
    name = "endsWith",
    description = "Indicates whether argument string ends with the specified suffix")
  public static boolean endsWith(
    @ElParam("string") String string,
    @ElParam("suffix") String suffix) {
    return string.endsWith(suffix);
  }

  @ElFunction(
      prefix = "str",
      name = "matches",
      description = "Tells whether the argument string matches the argument regex.")
  public static boolean matches(
      @ElParam("string") String string,
      @ElParam("regex") String regEx) {
    Utils.checkArgument(regEx != null, "Argument regEx for str:matches() cannot be null.");
    return string != null && string.matches(regEx);
  }

  @ElFunction(
      prefix = "str",
      name = "concat",
      description = "Returns a new string that is a concatenation of the two argument strings.")
  public static String concat(
      @ElParam("string1") String string1,
      @ElParam("string2") String string2) {
    string1 = (string1 == null)? "" : string1;
    string2 = (string2 == null)? "" : string2;
    return string1.concat(string2);
  }

  @ElFunction(
      prefix = "str",
      name = "length",
      description = "Returns the string length of the string argument."
  )
  public static int len (
      @ElParam("string") String string
  ) {
    string = (string == null)? "" : string;
    return string.length();
  }

  @ElFunction(
      prefix = "str",
      name = "urlEncode",
      description = "Returns URL encoded variant of the string."
  )
  public static String urlEncode (
      @ElParam("string") String string,
      @ElParam("encoding") String encoding
  ) throws UnsupportedEncodingException {
    if(string == null) {
      return null;
    }

    return URLEncoder.encode(string, encoding);
  }

  @ElFunction(
      prefix = "str",
      name = "urlDecode",
      description = "Returns decoded string from URL encoded variant."
  )
  public static String urlDecode (
      @ElParam("string") String string,
      @ElParam("encoding") String encoding
  ) throws UnsupportedEncodingException {
    if(string == null) {
      return null;
    }

    return URLDecoder.decode(string, encoding);
  }

  @ElFunction(
      prefix = "str",
      name = "escapeXML10",
      description = "Returns a string safe to embed in an XML 1.0 or 1.1 document."
  )
  public static String escapeXml10(@ElParam("string") String string) {
    return StringEscapeUtils.escapeXml10(string);
  }

  @ElFunction(
      prefix = "str",
      name = "escapeXML11",
      description = "Returns a string safe to embed in an XML 1.1 document."
  )
  public static String escapeXml11(@ElParam("string") String string) {
    return StringEscapeUtils.escapeXml11(string);
  }

  @ElFunction(
      prefix = "str",
      name = "unescapeXML",
      description = "Returns an unescaped string from a string with XML special characters escaped."
  )
  public static String unescapeXml(@ElParam("string") String string) {
    return StringEscapeUtils.unescapeXml(string);
  }

  @ElFunction(
      prefix = "str",
      name = "unescapeJava",
      description = "Returns an unescaped string from a string with Java special characters (e.g. \\n will be converted to 0x0A)."
  )
  public static String java(@ElParam("string") String string) {
    return StringEscapeUtils.unescapeJava(string);
  }


  @ElFunction(
      prefix = "list",
      name = "join",
      description = "Returns each element of a LIST field joined on the specified character sequence."
  )
  public static String joinList(
      @ElParam("list") List<Field> list,
      @ElParam("separator") String separator
  ) {
    if(list == null) {
      return "";
    }

    List<String> listOfStrings = list.stream()
        .map(field -> field.getValue() == null ? "null" : field.getValueAsString())
        .collect(Collectors.toList());
    return Joiner.on(separator).join(listOfStrings);
  }

  @ElFunction(
      prefix = "list",
      name = "joinSkipNulls",
      description = "Returns each element of a LIST field joined on the specified character sequence skipping null " +
          "values."
  )
  public static String joinListSkipNulls(
      @ElParam("list") List<Field> list,
      @ElParam("separator") String separator
  ) {
    if(list == null) {
      return "";
    }

    List<String> listOfStrings = list.stream()
        .map(field -> field.getValue() == null ? null : field.getValueAsString())
        .collect(Collectors.toList());
    return Joiner.on(separator).skipNulls().join(listOfStrings);
  }

  @ElFunction(
      prefix = "map",
      name = "join",
      description = "Returns each element of a LIST field joined on the specified character sequence."
  )
  public static String joinMap(
      @ElParam("map") Map<String, Field> map,
      @ElParam("separator") String separator,
      @ElParam("keyValueSeparator") String kvSeparator
  ) {
    if(map == null) {
      return "";
    }

    Map<String, String> mapOfStrings = map.entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> e.getValue().getValue() == null ? "null" : e.getValue().getValueAsString()
        ));
    Joiner joiner = Joiner.on(separator);

    return joiner.withKeyValueSeparator(kvSeparator).join(mapOfStrings);
  }

  /*
   * As generating UUID is expensive operation, we spawn a thread that will pre-generate them
   */
  private static SecureRandom randomGenerator = new SecureRandom();
  private static BlockingQueue<String> uuidQueue =
    new ArrayBlockingQueue<>(Integer.parseInt(System.getProperty("com.streamsets.pipeline.lib.el.StringEL.uuid_queue_max", "10000")));
  private static ExecutorService executor = Executors.newFixedThreadPool(1);
  static {
    executor.submit(() -> {
      Thread.currentThread().setName("UUID Pre generation thread");
      while(true) {
        try {
          uuidQueue.put(UUID.nameUUIDFromBytes(randomGenerator.generateSeed(16)).toString());
        } catch (InterruptedException e) {
          // Ignored
        }
      }
    });
  }

  @ElFunction(
      prefix = "uuid",
      name = "uuid",
      description = "Returns a randomly generated UUID."
  )
  public static String uuid() throws InterruptedException {
    return uuidQueue.take();
  }

  @ElFunction(
      prefix = "str",
      name = "splitKV",
      description = "Splits key value pairs into a map"
  )
  public static Map<String, Field> splitKV(
      @ElParam("string") String string,
      @ElParam("pairSeparator") String separator,
      @ElParam("kvSeparator") String kvSeparator) {
    if (Strings.isNullOrEmpty(string)) {
      return Collections.emptyMap();
    }

    Splitter.MapSplitter splitter = Splitter.on(separator).trimResults().omitEmptyStrings().withKeyValueSeparator(kvSeparator);

    return splitter.split(string).entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> Field.create(e.getValue())));
  }

}
