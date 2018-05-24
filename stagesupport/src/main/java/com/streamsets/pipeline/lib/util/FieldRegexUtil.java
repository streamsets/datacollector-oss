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
package com.streamsets.pipeline.lib.util;

import com.streamsets.datacollector.record.PathElement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FieldRegexUtil {

  //This will handle list Fields with wild card. For ex:/list[*]
  //(We do not need to support /list[\\d+]) as * will cover for it (as array indices are just numbers)
  //replace it with /list\[\d+\].
  //We currently won't support selectively selecting some array indices with regex. For EX: /list[([0-9])]
  //Move the first 10 indices.
  private static final Pattern ARRAY_IDX_WILD_CARD_REGEX_PATTERN = Pattern.compile("\\[(\\(?)(\\*)(\\)?)\\]");
  private static final String ARRAY_IDX_WILD_CARD_REPLACE_STRING = "\\\\[$1\\\\d+$3\\\\]";
  //This will handle list element specified with constant index /list[0], /list[1], /list[(0)], /list[(1)]
  //and replace it with just escaping the array index bracket, /list\[0\], /list\[1\], /list\[(0\], /list\[(1\]
  //respectively.
  private static final Pattern ARRAY_IDX_CONST_NUM_REGEX_PATTERN = Pattern.compile("\\[(\\(?)(\\d+)(\\)?)\\]");
  private static final String ARRAY_IDX_CONST_NUM_REPLACE_STRING = "\\\\[$1$2$3\\\\]";
  //This will handle map fields with wild card. For ex: /map/*/field, /map/(*)/field
  //And replace it with /map/[^\/[]+/field and  /map/([^\/[]+)/field respectively.
  private static final Pattern MAP_WILDCARD_FIELD_PATTERN = Pattern.compile("(\\/\\(?)\\*(\\)?)");
  private static final String MAP_WILD_CARD_REPLACEMENT = "$1[^\\\\/\\\\[]+$2";

  private static final String BRACKETED_WILDCARD_ANY_LENGTH = "[" + PathElement.WILDCARD_ANY_LENGTH + "]";
  private FieldRegexUtil() {}

  public static boolean hasWildCards(String fieldPath) {
    if(
        fieldPath.contains(BRACKETED_WILDCARD_ANY_LENGTH) || fieldPath.contains("/" + PathElement.WILDCARD_ANY_LENGTH)
        || fieldPath.contains(PathElement.WILDCARD_ANY_LENGTH) || fieldPath.contains(PathElement.WILDCARD_SINGLE_CHAR)
    ) {
      return true;
    }
    return false;
  }

  public static List<String> getMatchingFieldPaths(String fieldPath, Iterable<String> fieldPaths) {
    if(!hasWildCards(fieldPath)) {
      return Arrays.asList(fieldPath);
    }

    //Any reference to array index brackets [ ] must be escaped in the regex
    //Reference to * in map must be replaced by regex that matches a field name
    //Reference to * in array index must be replaced by \d+


    fieldPath = transformFieldPathRegex(fieldPath);

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

  public static String transformFieldPathRegex(String fieldPath) {
    return fieldPath
        .replace(BRACKETED_WILDCARD_ANY_LENGTH, "[\\d+]")
        .replace("[", "\\[")
        .replace("]", "\\]")
        .replaceAll("\\/\\*", "/([^\\\\/\\\\[]+)")
        .replaceAll(Pattern.quote(PathElement.WILDCARD_ANY_LENGTH), "\\\\w+")
        .replaceAll(Pattern.quote(PathElement.WILDCARD_SINGLE_CHAR), "\\\\w");
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
        ARRAY_IDX_WILD_CARD_REGEX_PATTERN,
        ARRAY_IDX_WILD_CARD_REPLACE_STRING
    );
    returnPath = patchUpSpecialCases(
        returnPath,
        ARRAY_IDX_CONST_NUM_REGEX_PATTERN,
        ARRAY_IDX_CONST_NUM_REPLACE_STRING
    );
    returnPath = patchUpSpecialCases(
        returnPath,
        MAP_WILDCARD_FIELD_PATTERN,
        MAP_WILD_CARD_REPLACEMENT
    );
    return returnPath;
  }

}
