/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.impl.Utils;

import java.lang.reflect.Method;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ELStringSupport {

  private static final String STRING_CONTEXT_VAR = "str";

  public static String substring(String string, int beginIndex, int endIndex) {
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

  public static String trim(String string) {
    return string.trim();
  }

  public static String toUpper(String string) {
    return string.toUpperCase();
  }

  public static String toLower(String string) {
    return string.toLowerCase();
  }

  public static String replace(String string, char oldChar, char newChar) {
    return string.replace(oldChar, newChar);
  }

  public static String replaceAll(String string, String regEx, String replacement) {
    return string.replaceAll(regEx, replacement);
  }

  public static String truncate(String string, int endIndex) {
    return string.substring(0, endIndex);
  }

  public static String regExCapture(String string, String regEx, int groupNumber) {
    Pattern pattern = Pattern.compile(regEx);
    Matcher matcher = pattern.matcher(string);
    if(matcher.find()) {
      return matcher.group(groupNumber);
    }
    return null;
  }

  public static boolean contains(String s1, String s2) {
    return s1.contains(s2);
  }

  public static boolean startsWith(String s1, String s2) {
    return s1.startsWith(s2);
  }

  public static boolean endsWith(String s1, String s2) {
    return s1.endsWith(s2);
  }

  private static final Method SUBSTRING;
  private static final Method TRIM;
  private static final Method TO_UPPER;
  private static final Method TO_LOWER;
  private static final Method REPLACE;
  private static final Method REPLACE_ALL;
  private static final Method TRUNCATE;
  private static final Method REGEX_CAPTURE;
  private static final Method CONTAINS;
  private static final Method STARTS_WITH;
  private static final Method ENDS_WITH;

  static {
    try {
      SUBSTRING = ELStringSupport.class.getMethod("substring", String.class, int.class, int.class);
      TRIM = ELStringSupport.class.getMethod("trim", String.class);
      TO_UPPER = ELStringSupport.class.getMethod("toUpper", String.class);
      TO_LOWER = ELStringSupport.class.getMethod("toLower", String.class);
      REPLACE = ELStringSupport.class.getMethod("replace", String.class, char.class, char.class);
      REPLACE_ALL = ELStringSupport.class.getMethod("replaceAll", String.class, String.class, String.class);
      TRUNCATE = ELStringSupport.class.getMethod("truncate", String.class, int.class);
      REGEX_CAPTURE = ELStringSupport.class.getMethod("regExCapture", String.class, String.class, int.class);
      CONTAINS = ELStringSupport.class.getMethod("contains", String.class, String.class);
      STARTS_WITH = ELStringSupport.class.getMethod("startsWith", String.class, String.class);
      ENDS_WITH = ELStringSupport.class.getMethod("endsWith", String.class, String.class);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void registerStringFunctions(ELEvaluator elEvaluator) {
    Utils.checkNotNull(elEvaluator, "elEvaluator");
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "substring", SUBSTRING);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "trim", TRIM);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "toUpper", TO_UPPER);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "toLower", TO_LOWER);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "replace", REPLACE);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "replaceAll", REPLACE_ALL);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "truncate", TRUNCATE);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "regExCapture", REGEX_CAPTURE);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "contains", CONTAINS);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "startsWith", STARTS_WITH);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "endsWith", ENDS_WITH);
  }

}
