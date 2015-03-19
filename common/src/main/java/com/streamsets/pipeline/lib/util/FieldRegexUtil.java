/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FieldRegexUtil {

  public static boolean hasWildCards(String fieldPath) {
    if(fieldPath.contains("[*]") || fieldPath.contains("/*")) {
      return true;
    }
    return false;
  }

  public static List<String> getMatchingFieldPaths(String fieldPath, Record record) {
    if(!hasWildCards(fieldPath)) {
      return ImmutableList.of(fieldPath);
    }
    fieldPath = fieldPath.replaceAll("\\/\\*", "/(.*)").replace("[*]", "\\[\\d+\\]");
    Pattern pattern = Pattern.compile(fieldPath);
    List<String> matchingFieldPaths = new ArrayList<>();
    for(String existingFieldPath : record.getFieldPaths()) {
      Matcher matcher = pattern.matcher(existingFieldPath);
      if(matcher.matches()) {
        matchingFieldPaths.add(existingFieldPath);
      }
    }
    return matchingFieldPaths;
  }
}
