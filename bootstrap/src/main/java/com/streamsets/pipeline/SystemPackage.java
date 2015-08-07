/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
