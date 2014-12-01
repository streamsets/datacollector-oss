/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.el;

import com.google.common.base.Preconditions;

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
    Preconditions.checkNotNull(elEvaluator, "elEvaluator cannot be null");
    elEvaluator.registerFunction("", "isIn", IS_IN);
    elEvaluator.registerFunction("", "notIn", NOT_IN);
  }

}
