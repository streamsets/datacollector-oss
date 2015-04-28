/**
 * Copied from YARN
 *
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
package com.streamsets.pipeline;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A {@link URLClassLoader} for application isolation. Classes from the
 * application JARs are loaded in preference to the parent loader.
 */
public class StageClassLoader extends BlackListURLClassLoader {
  /**
   * Default value of the system classes if the user did not override them.
   * JDK classes, hadoop classes and resources, and some select third-party
   * classes are considered system classes, and are not loaded by the
   * application classloader.
   */
  public static final String API_CLASSES_DEFAULT;
  public static final String CONTAINER_CLASSES_DEFAULT;
  public static final String STAGE_CLASSES_DEFAULT;
  private static final String BASE_CLASSES_DEFAULT;
  private static String API = "api";
  private static String BASE = "base";
  private static String CONTAINER = "container";
  private static String STAGE = "stage";
  private static final String[] CLASSLOADER_TYPES = new String[] {
    API, BASE, CONTAINER, STAGE
  };

  private static final String SYSTEM_CLASSES_DEFAULT_KEY =
    "system.classes.default";

  private static boolean debug = true;

  public static void setDebug(boolean debug) {
    StageClassLoader.debug = debug;
  }

  static {
    Map<String, String> classesDefaultsMap = new HashMap<>();
    for (String classLoaderType : CLASSLOADER_TYPES) {
      String propertiesFile = classLoaderType + "-classloader.properties";
      try (InputStream is = StageClassLoader.class.getClassLoader()
        .getResourceAsStream(propertiesFile);) {
        if (is == null) {
          throw new ExceptionInInitializerError("properties file " +
            propertiesFile + " is not found");
        }
        Properties props = new Properties();
        props.load(is);
        // get the system classes default
        String systemClassesDefault =
          props.getProperty(SYSTEM_CLASSES_DEFAULT_KEY);
        if (systemClassesDefault == null) {
          throw new ExceptionInInitializerError("property " +
            SYSTEM_CLASSES_DEFAULT_KEY + " is not found");
        }
        classesDefaultsMap.put(classLoaderType, systemClassesDefault);
      } catch (IOException e) {
        throw new ExceptionInInitializerError(e);
      }
    }
    BASE_CLASSES_DEFAULT = checkNotNull(classesDefaultsMap.get(BASE), BASE);
    API_CLASSES_DEFAULT = checkNotNull(classesDefaultsMap.get(API), API) + "," + BASE_CLASSES_DEFAULT;
    CONTAINER_CLASSES_DEFAULT = checkNotNull(classesDefaultsMap.get(CONTAINER), CONTAINER) + "," + BASE_CLASSES_DEFAULT;
    STAGE_CLASSES_DEFAULT = checkNotNull(classesDefaultsMap.get(STAGE), STAGE) + "," + BASE_CLASSES_DEFAULT;
  }

  private final ClassLoader parent;
  private final List<String> systemClasses;

  private StageClassLoader(String type, String name, List<URL> urls, ClassLoader parent,
                          List<String> systemClasses, String[] blacklistedPackages) {
    super(type, name, urls, parent, blacklistedPackages);
    if (debug) {
      System.err.println(getClass().getSimpleName() + ": urls: " + Arrays.toString(urls.toArray()));
      System.err.println(getClass().getSimpleName() + ": system classes: " + systemClasses);
    }
    this.parent = parent;
    if (parent == null) {
      throw new IllegalArgumentException("No parent classloader!");
    }
    if (debug) {
      System.err.println(getClass().getSimpleName() + ": parent classloader: " + parent);
    }
    if (systemClasses == null) {
      throw new IllegalArgumentException("System classes cannot be null");
    }
    // if the caller-specified system classes are null or empty, use the default
    this.systemClasses = systemClasses;
    if(debug) {
      System.err.println(getClass().getSimpleName() + ": system classes: " + this.systemClasses);
    }
  }
  public StageClassLoader(String type, String name, List<URL> urls, ClassLoader parent, String[] blacklistedPackages,
                          String systemClasses) {
    this(type, name, urls, parent, Arrays.asList(getTrimmedStrings(systemClasses)), blacklistedPackages);
  }
  public StageClassLoader(String type, String name, List<URL> urls, ClassLoader parent, String[] blacklistedPackages) {
    this(type, name, urls, parent, Collections.<String>emptyList(), blacklistedPackages);
  }

  @Override
  public URL getResource(String name) {
    URL url = null;

    if (!isSystemClass(name, systemClasses)) {
      url= findResource(name);
      if (url == null && name.startsWith("/")) {
        if (debug) {
          System.err.println(getClass().getSimpleName() + ": Remove leading / off " + name);
        }
        url = findResource(name.substring(1));
      }
    }

    if (url == null) {
      url = parent.getResource(name);
    }

    if (url != null) {
      if (debug) {
        System.err.println(getClass().getSimpleName() + ": getResource(" + name + ")=" + url);
      }
    }

    return url;
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return this.loadClass(name, false);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve)
    throws ClassNotFoundException {
    if (debug) {
      System.err.println(getClass().getSimpleName() + ": Loading class: " + name);
    }

    Class<?> c = findLoadedClass(name);
    ClassNotFoundException ex = null;

    if (c == null && !isSystemClass(name, systemClasses)) {
      // Try to load class from this classloader's URLs. Note that this is like
      // the servlet spec, not the usual Java 2 behaviour where we ask the
      // parent to attempt to load first.
      try {
        c = findClass(name);
        if (debug && c != null) {
          System.err.println(getClass().getSimpleName() + ": Loaded class: " + name + " ");
        }
      } catch (ClassNotFoundException e) {
        if (debug) {
          System.err.println(getClass().getSimpleName() + ": " + e);
        }
        ex = e;
      }
    }

    if (c == null) { // try parent
      c = parent.loadClass(name);
      if (debug && c != null) {
        System.err.println(getClass().getSimpleName() + ": Loaded class from parent: " + name + " ");
      }
    }

    if (c == null) {
      throw ex != null ? ex : new ClassNotFoundException(name);
    }

    if (resolve) {
      resolveClass(c);
    }

    return c;
  }

  /**
   * Checks if a class should be included as a system class.
   *
   * A class is a system class if and only if it matches one of the positive
   * patterns and none of the negative ones.
   *
   * @param name the class name to check
   * @param systemClasses a list of system class configurations.
   * @return true if the class is a system class
   */
  public static boolean isSystemClass(String name, List<String> systemClasses) {
    boolean result = false;
    if (systemClasses != null) {
      String canonicalName = name.replace('/', '.');
      while (canonicalName.startsWith(".")) {
        canonicalName=canonicalName.substring(1);
      }
      for (String c : systemClasses) {
        boolean shouldInclude = true;
        if (c.startsWith("-")) {
          c = c.substring(1);
          shouldInclude = false;
        }
        if (canonicalName.startsWith(c)) {
          if (   c.endsWith(".")                                   // package
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

  private static <T> T checkNotNull(T value, String name) {
    if (value == null) {
      throw new NullPointerException("Value " + name + " is null");
    }
    return value;
  }

  private static String[] getTrimmedStrings(String str){
    if (null == str || str.trim().isEmpty()) {
      return new String[0];
    }
    return str.trim().split("\\s*,\\s*");
  }
}