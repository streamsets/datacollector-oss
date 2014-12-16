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

  private static final Method IS_IN;
  private static final Method NOT_IN;

  static {
    try {
      IS_IN = ELBasicSupport.class.getMethod("isIn", String.class, String.class);
      NOT_IN = ELBasicSupport.class.getMethod("notIn", String.class, String.class);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void registerBasicFunctions(ELEvaluator elEvaluator) {
    Utils.checkNotNull(elEvaluator, "elEvaluator");
    elEvaluator.registerFunction("", "isIn", IS_IN);
    elEvaluator.registerFunction("", "notIn", NOT_IN);
  }

}
