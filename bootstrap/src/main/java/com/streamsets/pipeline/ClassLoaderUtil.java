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
package com.streamsets.pipeline;

public class ClassLoaderUtil {

  static final String SERVICES_PREFIX = "/META-INF/services/";

  private static final String SERVICES_PREFIX_CANONICALIZED = canonicalizeClass(SERVICES_PREFIX);
  private static final String[] EMPTY_STRING_ARRAY = new String[0];

  private ClassLoaderUtil() {}

  static String canonicalizeClass(String name) {
    String canonicalName = name.replace('/', '.');
    while (canonicalName.startsWith(".")) {
      canonicalName = canonicalName.substring(1);
    }
    return canonicalName;
  }

  static String canonicalizeClassOrResource(String name) {
    String canonicalName = canonicalizeClass(name);
    if (canonicalName.startsWith(SERVICES_PREFIX_CANONICALIZED)) {
      canonicalName = canonicalName.substring(SERVICES_PREFIX_CANONICALIZED.length());
    }
    return canonicalName;
  }

  static <T> T checkNotNull(T value, String name) {
    if (value == null) {
      throw new NullPointerException("Value " + name + " is null");
    }
    return value;
  }

  static String[] getTrimmedStrings(String str){
    if (null == str) {
      return EMPTY_STRING_ARRAY;
    }
    if (str.isEmpty()) {
      return EMPTY_STRING_ARRAY;
    }
    return str.trim().split("\\s*,\\s*");
  }
}
