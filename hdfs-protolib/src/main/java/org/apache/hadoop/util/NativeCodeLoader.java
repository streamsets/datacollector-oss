/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package org.apache.hadoop.util;

public class NativeCodeLoader {

  public static boolean isNativeCodeLoaded() {
    return false;
  }

  public static boolean buildSupportsSnappy() {
    return false;
  }

  public static boolean buildSupportsOpenssl() {
    return false;
  }

  public static String getLibraryName() {
    return "<NO NATIVE LIB>";
  }


}
