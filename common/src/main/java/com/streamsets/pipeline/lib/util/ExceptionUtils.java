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

public class ExceptionUtils {

  private ExceptionUtils() {}

  /**
   * Throws an undeclared checked exception, use with caution.
   */
  public static void throwUndeclared(Throwable ex) {
    ExceptionUtils.<RuntimeException>_throw(ex);
  }

  @SuppressWarnings("unchecked")
  private static <E extends Exception> void _throw(Throwable e) throws E {
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
