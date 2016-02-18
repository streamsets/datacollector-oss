/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringEL {
  public static final String MEMOIZED = "memoized";

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
    name = "trim",
    description = "Removes leading and trailing whitespaces")
  public static String trim(
    @ElParam("string") String string) {
    return string.trim();
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
    description = "Returns a new string resulting from replacing all occurrences of oldChar in this string with newChar")
  public static String replace(
    @ElParam("string") String string,
    @ElParam("oldChar") char oldChar,
    @ElParam("newChar") char newChar) {
    return string.replace(oldChar, newChar);
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

}
