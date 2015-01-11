/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.impl.Utils;

import java.lang.reflect.Method;

public class ELBasicSupport {

  // list of values must be comma separated
  public static boolean isIn(String str, String listOfValues) {
    String[] values = listOfValues.split(",");
    for (String value : values) {
      if (value.equals(str)) {
        return true;
      }
    }
    return false;
  }

  public static boolean notIn(String str, String listOfValues) {
    String[] values = listOfValues.split(",");
    for (String value : values) {
      if (value.equals(str)) {
        return false;
      }
    }
    return true;
  }

  public static String concat(String... strings) {
    StringBuilder stringBuilder = new StringBuilder();
    for (String string : strings) {
      stringBuilder.append(string);
    }
    return stringBuilder.toString();
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

  private static final Method IS_IN;
  private static final Method NOT_IN;
  private static final Method CONCAT;
  private static final Method SUBSTRING;

  static {
    try {
      IS_IN = ELBasicSupport.class.getMethod("isIn", String.class, String.class);
      NOT_IN = ELBasicSupport.class.getMethod("notIn", String.class, String.class);
      CONCAT = ELBasicSupport.class.getMethod("concat", String[].class);
      SUBSTRING = ELBasicSupport.class.getMethod("substring", String.class, int.class, int.class);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void registerBasicFunctions(ELEvaluator elEvaluator) {
    Utils.checkNotNull(elEvaluator, "elEvaluator");
    elEvaluator.registerFunction("", "isIn", IS_IN);
    elEvaluator.registerFunction("", "notIn", NOT_IN);
    elEvaluator.registerFunction("", "concat", CONCAT);
    elEvaluator.registerFunction("", "substring", SUBSTRING);
  }

}
