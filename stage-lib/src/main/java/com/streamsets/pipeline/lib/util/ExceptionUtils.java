/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

public class ExceptionUtils {

  /**
   * Throws an undeclared checked exception, use with caution.
   */
  public static void throwUndeclared(Exception ex) {
    ExceptionUtils.<RuntimeException>_throw(ex);
  }

  @SuppressWarnings("unchecked")
  private static <E extends Exception> void _throw(Exception e) throws E {
    throw (E)e;
  }

  @SuppressWarnings("unchecked")
  public static <E extends Throwable> E findSpecificCause(Exception ex, Class<E> causeClass) {
    Throwable cause = ex.getCause();
    while (cause != null && ! causeClass.isInstance(cause)) {
      cause = cause.getCause();
    }
    return (E) cause;
  }

}
