/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FieldRegexUtil {

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
}
