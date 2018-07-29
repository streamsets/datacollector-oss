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

import java.util.Arrays;
import java.util.List;

public class SystemPackage {
  private final List<String> packages;

  public SystemPackage(String[] packages) {
    this(Arrays.asList(packages));
  }

  public SystemPackage(List<String> packages) {
    this.packages = packages;
  }


  public boolean isSystem(String packageName) {
    return isSystemClass(packageName, packages);
  }

  /**
   * Checks if a class should be included as a system class.
   *
   * A class is a system class if and only if it matches one of the positive
   * patterns and none of the negative ones.
   *
   * @param name the class name to check
   * @param packageList a list of system class configurations.
   * @return true if the class is a system class
   */
  private static boolean isSystemClass(String name, List<String> packageList) {
    boolean result = false;
    if (packageList != null) {
      String canonicalName = ClassLoaderUtil.canonicalizeClassOrResource(name);
      for (String c : packageList) {
        boolean shouldInclude = true;
        if (c.startsWith("-")) {
          c = c.substring(1);
          shouldInclude = false;
        }
        if (canonicalName.startsWith(c)) {
          if ( c.endsWith(".")                                   // package
            || canonicalName.length() == c.length()              // class
            ||    canonicalName.length() > c.length()            // nested
            && canonicalName.charAt(c.length()) == '$' ) {
            if (shouldInclude) {
              result = true;
            } else {
              return false;
            }
          }
        }
      }
    }
    return result;
  }

}
