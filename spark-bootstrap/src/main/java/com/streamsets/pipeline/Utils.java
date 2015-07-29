/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline;

public class Utils {

  public static <T> T checkNotNull(T value, Object varName) {
    if (value == null) {
      throw new NullPointerException(varName + " cannot be null");
    }
    return value;
  }

  public static <T> T  checkArgumentNotNull(T arg, Object msg) {
    if (arg == null) {
      throw new IllegalArgumentException((msg != null) ? msg.toString() : "");
    }
    return arg;
  }
}
