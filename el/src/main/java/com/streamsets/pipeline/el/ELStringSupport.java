/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.impl.Utils;

import java.lang.reflect.Method;

public class ELStringSupport {

  private static final String STRING_CONTEXT_VAR = "str";

  public static String concat2(String string1, String string2) {
    return string1 + string2;
  }

  public static String concat3(String string1, String string2, String string3) {
    return string1 + string2 + string3;
  }

  public static String concat4(String string1, String string2, String string3, String string4) {
    return string1 + string2 + string3 + string4;
  }

  public static String concat5(String string1, String string2, String string3, String string4, String string5) {
    return string1 + string2 + string3 + string4 + string5;
  }

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

  private static final Method CONCAT2;
  private static final Method CONCAT3;
  private static final Method CONCAT4;
  private static final Method CONCAT5;
  private static final Method SUBSTRING;
  private static final Method TRIM;
  private static final Method TO_UPPER;
  private static final Method TO_LOWER;
  private static final Method REPLACE;
  private static final Method REPLACE_ALL;
  private static final Method TRUNCATE;

  static {
    try {
      CONCAT2 = ELStringSupport.class.getMethod("concat2", String.class, String.class);
      CONCAT3 = ELStringSupport.class.getMethod("concat3", String.class, String.class, String.class);
      CONCAT4 = ELStringSupport.class.getMethod("concat4", String.class, String.class, String.class, String.class);
      CONCAT5 = ELStringSupport.class.getMethod("concat5", String.class, String.class, String.class, String.class,
        String.class);
      SUBSTRING = ELStringSupport.class.getMethod("substring", String.class, int.class, int.class);
      TRIM = ELStringSupport.class.getMethod("trim", String.class);
      TO_UPPER = ELStringSupport.class.getMethod("toUpper", String.class);
      TO_LOWER = ELStringSupport.class.getMethod("toLower", String.class);
      REPLACE = ELStringSupport.class.getMethod("replace", String.class, char.class, char.class);
      REPLACE_ALL = ELStringSupport.class.getMethod("replaceAll", String.class, String.class, String.class);
      TRUNCATE = ELStringSupport.class.getMethod("truncate", String.class, int.class);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void registerStringFunctions(ELEvaluator elEvaluator) {
    Utils.checkNotNull(elEvaluator, "elEvaluator");
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "concat2", CONCAT2);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "concat3", CONCAT3);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "concat4", CONCAT4);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "concat5", CONCAT5);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "substring", SUBSTRING);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "trim", TRIM);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "toUpper", TO_UPPER);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "toLower", TO_LOWER);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "replace", REPLACE);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "replaceAll", REPLACE_ALL);
    elEvaluator.registerFunction(STRING_CONTEXT_VAR, "truncate", TRUNCATE);
  }

}
