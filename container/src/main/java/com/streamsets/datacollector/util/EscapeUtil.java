/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EscapeUtil {
  public static Pattern pattern = Pattern.compile("\\W", Pattern.CASE_INSENSITIVE);

  public static String singleQuoteEscape(String path) {
    if(path != null) {
      Matcher matcher = pattern.matcher(path);
      if(matcher.find()) {
        path = path.replace("\\", "\\\\")
          .replace("\"", "\\\"")
          .replace("'", "\\\\\'");
        return "'" + path + "'";
      }
    }
    return path;
  }

  public static String singleQuoteUnescape(String path) {
    if(path != null) {
      Matcher matcher = pattern.matcher(path);
      if(matcher.find() && path.length() > 2) {
        path = path.replace("\\\"", "\"")
          .replace("\\\\\'", "'")
          .replace("\\\\", "\\");
        return path.substring(1, path.length() - 1);
      }
    }
    return path;
  }

  public static String doubleQuoteEscape(String path) {
    if(path != null) {
      Matcher matcher = pattern.matcher(path);
      if(matcher.find()) {
        path = path.replace("\\", "\\\\")
          .replace("\"", "\\\\\"")
          .replace("'", "\\\'");
        return "\"" + path + "\"";
      }
    }
    return path;
  }

  public static String doubleQuoteUnescape(String path) {
    if(path != null) {
      Matcher matcher = pattern.matcher(path);
      if(matcher.find() && path.length() > 2) {
        path = path.replace("\\\\\"", "\"")
          .replace("\\\'", "'")
          .replace("\\\\", "\\");
        return path.substring(1, path.length() - 1);
      }
    }
    return path;
  }

  /**
   * This method is used during deserializer and sqpath (Single quote escaped path) is passed to determine the last field name.
   */
  public static String getLastFieldNameFromPath(String path) {
    String [] pathSplit = (path != null) ? path.split("/") : null;
    if(pathSplit != null && pathSplit.length > 0) {
      String lastFieldName = pathSplit[pathSplit.length - 1];

      //handle special case field name containing slash eg. /'foo/bar'
      if(lastFieldName.contains("'") &&
        !(lastFieldName.charAt(0) == '\'' && lastFieldName.charAt(lastFieldName.length() - 1) == '\'')) {

        //If path contains slash inside name, split it by "/'"
        pathSplit = path.split("/'");
        if(pathSplit.length > 0) {
          lastFieldName = "'" + pathSplit[pathSplit.length - 1];
        }
      }

      return EscapeUtil.singleQuoteUnescape(lastFieldName);
    }
    return path;
  }
}
