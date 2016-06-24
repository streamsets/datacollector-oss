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
package com.streamsets.pipeline.lib.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FieldRegexUtil {
  private static final Pattern ARRAY_IDX_REGEX_PATTERN = Pattern.compile("\\[(\\(?)(\\*|\\d+)(\\)?)\\]");
  private static final String ARRAY_IDX_REPLACE_STRING = "\\\\[$1\\\\d+$3\\\\]";
  private static final Pattern MAP_WILDCARD_FIELD_PATTERN = Pattern.compile("(\\/\\(?)\\*(\\)?)");
  private static final String MAP_WILD_CARD_REPLACEMENT = "$1[^\\\\/\\\\[]+$2";

  private FieldRegexUtil() {}

  public static boolean hasWildCards(String fieldPath) {
    if(fieldPath.contains("[*]") || fieldPath.contains("/*")) {
      return true;
    }
    return false;
  }

  public static List<String> getMatchingFieldPaths(String fieldPath, Set<String> fieldPaths) {
    if(!hasWildCards(fieldPath)) {
      return Arrays.asList(fieldPath);
    }
    //Any reference to array index brackets [ ] must be escaped in the regex
    //Reference to * in map must be replaced by regex that matches a field name
    //Reference to * in array index must be replaced by \d+
    fieldPath = fieldPath
        .replace("[*]", "[\\d+]")
        .replace("[", "\\[")
        .replace("]", "\\]")
        .replaceAll("\\/\\*", "/([^\\\\/\\\\[]+)");
    Pattern pattern = Pattern.compile(fieldPath);
    List<String> matchingFieldPaths = new ArrayList<>();
    for(String existingFieldPath : fieldPaths) {
      Matcher matcher = pattern.matcher(existingFieldPath);
      if(matcher.matches()) {
        matchingFieldPaths.add(existingFieldPath);
      }
    }
    return matchingFieldPaths;
  }

  private static String patchUpSpecialCases(String fieldPath, Pattern pattern, String replaceMentString) {
    Matcher matcher = pattern.matcher(fieldPath);
    if (matcher.find()) {
      return matcher.replaceAll(replaceMentString);
    }
    return fieldPath;
  }

  public static String patchUpFieldPathRegex(String fieldPath) {
    String returnPath = fieldPath;
    returnPath = patchUpSpecialCases(
        returnPath,
        ARRAY_IDX_REGEX_PATTERN,
        ARRAY_IDX_REPLACE_STRING
    );
    returnPath = patchUpSpecialCases(
        returnPath,
        MAP_WILDCARD_FIELD_PATTERN,
        MAP_WILD_CARD_REPLACEMENT
    );
    return returnPath;
  }
}
