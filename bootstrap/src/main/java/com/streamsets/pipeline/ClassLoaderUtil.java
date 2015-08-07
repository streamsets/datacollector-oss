/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ClassLoaderUtil {

  private static final String[] EMPTY_STRING_ARRAY = new String[0];
  static final String SERVICES_PREFIX = "/META-INF/services/";
  private static final String SERVICES_PREFIX_CANONICALIZED = canonicalizeClass(SERVICES_PREFIX);

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
