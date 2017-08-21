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
package com.streamsets.testing;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Helper methods for easily building parametrized runner data.
 */
public class ParametrizedUtils {

  /**
   * Create cross product of one or more arrays to create all combination of given
   * array of arrays.
   *
   * @param arrays Array of arrays
   * @return Cross product of all arrays
   */
  public static Collection<Object []> crossProduct(Object[] ...arrays) {
    LinkedList<Object []> ret = new LinkedList<Object []>();
    crossProductInternal(0, arrays, new Object[arrays.length], ret);
    return ret;
  }
  private static void crossProductInternal(int i, Object[][] arrays, Object []work, LinkedList<Object[]> ret) {
    if(i == arrays.length) {
      ret.add(Arrays.copyOf(work, work.length));
      return;
    }

    for(Object item: arrays[i]) {
      work[i] = item;
      crossProductInternal(i+1, arrays, work, ret);
    }
  }

  /**
   * Convert given array to "array of arrays".
   *
   * This method is particularly useful if one needs only one directional array to be given to
   * Parametrized runner.
   */
  public static Collection<Object []> toArrayOfArrays(Object ...array) {
    LinkedList<Object []> ret = new LinkedList<>();
    for(Object o : array) {
      ret.add(toArray(o));
    }
    return ret;
  }

  /**
   * If given object is an array, return it, otherwise wrap it in one-item array.
   */
  public static Object [] toArray(Object o) {
    if(o.getClass().isArray()) {
      return (Object [])o;
    }

    return new Object[] {o};
  }
}
