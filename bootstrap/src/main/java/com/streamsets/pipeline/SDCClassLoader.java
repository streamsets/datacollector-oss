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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A {@link URLClassLoader} for application isolation. There are two
 * configuration types supported by StageClassLoader. The first
 * is System classes which are always delegated to the parent
 * and the second is application classes which are never delegated
 * to the parent.
 */
public class SDCClassLoader extends BlackListURLClassLoader {

  /*
   * Note:
   * if you update this, you must also update stage-classloader.properties
   */
  private static final String[] PACKAGES_BLACKLIST_FOR_STAGE_LIBRARIES = {
      "com.streamsets.pipeline.api.",
      "com.streamsets.pipeline.container",
      "com.codehale.metrics.",
      "org.slf4j.",
      "org.apache.log4j."
  };

  /**
   * Default value of the system classes if the user did not override them.
   * JDK classes, hadoop classes and resources, and some select third-party
   * classes are considered system classes, and are not loaded by the
   * application classloader.
   */
  private static final String SYSTEM_API_CLASSES_DEFAULT;
  private static final String SYSTEM_CONTAINER_CLASSES_DEFAULT;
  private static final String SYSTEM_STAGE_CLASSES_DEFAULT;
  private static final String SYSTEM_BASE_CLASSES_DEFAULT;
  private static final String APPLICATION_API_CLASSES_DEFAULT;
  private static final String APPLICATION_CONTAINER_CLASSES_DEFAULT;
  private static final String APPLICATION_STAGE_CLASSES_DEFAULT;
  private static final String APPLICATION_BASE_CLASSES_DEFAULT;
  private static String API = "api";
  private static String BASE = "base";
  private static String CONTAINER = "container";
  private static String STAGE = "stage";
  private static final String[] CLASSLOADER_TYPES = new String[] {
    API, BASE, CONTAINER, STAGE
  };
  private static final String CLASS_FILE_SUFFIX = ".class";
  static final String SERVICES_PREFIX = "/META-INF/services/";

  private static final String SYSTEM_CLASSES_DEFAULT_KEY =
    "system.classes.default";

  private static final String APPLICATION_CLASSES_DEFAULT_KEY =
    "application.classes.default";

  private static boolean debug = false;

  public static void setDebug(boolean debug) {
    SDCClassLoader.debug = debug;
  }

  static {
    Map<String, String> systemClassesDefaultsMap = new HashMap<>();
    Map<String, String> applicationClassesDefaultsMap = new HashMap<>();
    for (String classLoaderType : CLASSLOADER_TYPES) {
      String propertiesFile = classLoaderType + "-classloader.properties";
      try (InputStream is = SDCClassLoader.class.getClassLoader()
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
        systemClassesDefaultsMap.put(classLoaderType, systemClassesDefault);
        applicationClassesDefaultsMap.put(classLoaderType, props.
          getProperty(APPLICATION_CLASSES_DEFAULT_KEY, "").trim());
      } catch (IOException e) {
        throw new ExceptionInInitializerError(e);
      }
    }
    SYSTEM_BASE_CLASSES_DEFAULT = checkNotNull(systemClassesDefaultsMap.get(BASE), BASE);
    SYSTEM_API_CLASSES_DEFAULT = checkNotNull(systemClassesDefaultsMap.get(API), API) + "," + SYSTEM_BASE_CLASSES_DEFAULT;
    SYSTEM_CONTAINER_CLASSES_DEFAULT = checkNotNull(systemClassesDefaultsMap.get(CONTAINER), CONTAINER) + "," + SYSTEM_BASE_CLASSES_DEFAULT;
    SYSTEM_STAGE_CLASSES_DEFAULT = checkNotNull(systemClassesDefaultsMap.get(STAGE), STAGE) + "," + SYSTEM_BASE_CLASSES_DEFAULT;
    APPLICATION_BASE_CLASSES_DEFAULT = checkNotNull(applicationClassesDefaultsMap.get(BASE), BASE);
    APPLICATION_API_CLASSES_DEFAULT = checkNotNull(applicationClassesDefaultsMap.get(API), API) + "," + APPLICATION_BASE_CLASSES_DEFAULT;
    APPLICATION_CONTAINER_CLASSES_DEFAULT = checkNotNull(applicationClassesDefaultsMap.get(CONTAINER), CONTAINER) + "," + APPLICATION_BASE_CLASSES_DEFAULT;
    APPLICATION_STAGE_CLASSES_DEFAULT = checkNotNull(applicationClassesDefaultsMap.get(STAGE), STAGE) + "," + APPLICATION_BASE_CLASSES_DEFAULT;
  }

  private final List<URL> urls;
  private final ClassLoader parent;
  private final List<String> systemClasses;
  private final List<String> applicationClasses;
  private final boolean isPrivate;

  private SDCClassLoader(String type, String name, List<URL> urls, ClassLoader parent,
                         List<String> systemClasses, List<String> applicationClasses, String[] blacklistedPackages,
      boolean isPrivate) {
    super(type, name, urls, parent, blacklistedPackages);
    this.urls = urls;
    if (debug) {
      System.err.println(getClass().getSimpleName() + " " + getName() + ": urls: " + Arrays.toString(urls.toArray()));
      System.err.println(getClass().getSimpleName() + " " + getName() + ": system classes: " + systemClasses);
    }
    this.parent = parent;
    if (parent == null) {
      throw new IllegalArgumentException("No parent classloader!");
    }
    if (debug) {
      System.err.println(getClass().getSimpleName() + " " + getName() + ": parent classloader: " + parent);
    }
    if (systemClasses == null) {
      throw new IllegalArgumentException("System classes cannot be null");
    }
    // if the caller-specified system classes are null or empty, use the default
    this.systemClasses = systemClasses;
    if(debug) {
      System.err.println(getClass().getSimpleName() + " " + getName() + ": system classes: " + this.systemClasses);
    }
    this.applicationClasses = applicationClasses;
    this.isPrivate = isPrivate;
    if(debug) {
      System.err.println(getClass().getSimpleName() + " " + getName() + ": application classes: " + this.applicationClasses);
    }
  }

  public SDCClassLoader(String type, String name, List<URL> urls, ClassLoader parent,
                        String[] blacklistedPackages, String systemClasses, String applicationClasses, boolean isPrivate) {
    this(type, name, urls, parent, Arrays.asList(getTrimmedStrings(systemClasses)),
      Arrays.asList(getTrimmedStrings(applicationClasses)), blacklistedPackages, isPrivate);
  }

  @Override
  public URL getResource(String name) {
    URL url = null;

    if (!isClassInList(name, systemClasses)) {
      url = findResource(name);
      if (url == null && name.startsWith("/")) {
        if (debug) {
          System.err.println(getClass().getSimpleName() + " " + getName() + ": Remove leading / off " + name);
        }
        url = findResource(name.substring(1));
      }
    }

    if (url == null && !isClassInList(name, applicationClasses)) {
      url = parent.getResource(name);
    }

    if (url != null) {
      if (debug) {
        System.err.println(getClass().getSimpleName() + " " + getName() + ": getResource(" + name + ")=" + url);
      }
    }

    return url;
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    if (debug) {
      System.err.println("getResources(" + name + ")");
    }
    Enumeration<URL> result = null;
    if (!isClassInList(name, systemClasses)) {
      // Search local repositories
      if (debug) {
        System.err.println("  Searching local repositories");
      }
      result = findResources(name);
      if (result != null && result.hasMoreElements()) {
        if (debug) {
          System.err.println("  --> Returning result from local");
        }
        return result;
      }
      if (isClassInList(name, applicationClasses)) {
        if (debug) {
          System.err.println("  --> application class, returning empty enumeration");
        }
        return Collections.emptyEnumeration();
      }
    }
    // Delegate to parent unconditionally
    if (debug) {
      System.err.println("  Delegating to parent classloader unconditionally " + parent);
    }
    result = parent.getResources(name);
    if (result != null && result.hasMoreElements()) {
      if (debug) {
        List<URL> resultList = Collections.list(result);
        result = Collections.enumeration(resultList);
        System.err.println("  --> Returning result from parent: " + resultList);
      }
      return result;
    }
    // (4) Resource was not found
    if (debug) {
      System.err.println("  --> Resource not found, returning empty enumeration");
    }
    return Collections.emptyEnumeration();
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    if (debug) {
      System.err.println("getResourceAsStream(" + name + ")");
    }
    InputStream stream = null;
    if (!isClassInList(name, systemClasses)) {
      // Search local repositories
      if (debug) {
        System.err.println("  Searching local repositories");
      }
      URL url = findResource(name);
      if (url != null) {
        if (debug) {
          System.err.println("  --> Returning stream from local");
        }
        try {
          return url.openStream();
        } catch (IOException e) {
          // Ignore
        }
      }
      if (isClassInList(name, applicationClasses)) {
        if (debug) {
          System.err.println("  --> application class, returning null");
        }
        return null;
      }
    }
    // Delegate to parent unconditionally
    if (debug) {
      System.err.println("  Delegating to parent classloader unconditionally " + parent);
    }
    stream = parent.getResourceAsStream(name);
    if (stream != null) {
      if (debug) {
        System.err.println("  --> Returning stream from parent");
      }
      return stream;
    }
    // (4) Resource was not found
    if (debug) {
      System.err.println("  --> Resource not found, returning null");
    }
    return null;
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return this.loadClass(name, false);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve)
    throws ClassNotFoundException {
    if (debug) {
      System.err.println(getClass().getSimpleName() + " " + getName() + ": Loading class: " + name);
    }

    Class<?> c = findLoadedClass(name);
    ClassNotFoundException ex = null;

    if (c == null && !isClassInList(name, systemClasses)) {
      // Try to load class from this classloader's URLs. Note that this is like
      // the servlet spec, not the usual Java 2 behaviour where we ask the
      // parent to attempt to load first.
      try {
        c = findClass(name);
        if (debug && c != null) {
          System.err.println(getClass().getSimpleName() + " " + getName() + ": Loaded class: " + name + " ");
        }
      } catch (ClassNotFoundException e) {
        if (debug) {
          System.err.println(getClass().getSimpleName() + " " + getName() + ": " + e);
        }
        ex = e;
      }
    }

    if (c == null && !isClassInList(name, applicationClasses)) { // try parent
      c = parent.loadClass(name);
      if (debug && c != null) {
        System.err.println(getClass().getSimpleName() + " " + getName() + ": Loaded class from parent: " + name + " ");
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
   * @param classList a list of system class configurations.
   * @return true if the class is a system class
   */
  public static boolean isClassInList(String name, List<String> classList) {
    boolean result = false;
    if (classList != null) {
      String canonicalName = canonicalize(name);
      String canonicalPrefix = canonicalize(SERVICES_PREFIX);
      if (canonicalName.startsWith(canonicalPrefix)) {
        canonicalName = canonicalName.substring(canonicalPrefix.length());
      }
      for (String c : classList) {
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

  private static String canonicalize(String canonicalName) {
    canonicalName = canonicalName.replace('/', '.');
    while (canonicalName.startsWith(".")) {
      canonicalName = canonicalName.substring(1);
    }
    return canonicalName;
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

  public static SDCClassLoader getAPIClassLoader(List<URL> apiURLs, ClassLoader parent) {
    return new SDCClassLoader("api-lib", "API", apiURLs, parent, null,
      SDCClassLoader.SYSTEM_API_CLASSES_DEFAULT, SDCClassLoader.APPLICATION_API_CLASSES_DEFAULT, false);
  }

  public static SDCClassLoader getContainerCLassLoader(List<URL> containerURLs, ClassLoader apiCL) {
    return new SDCClassLoader("container-lib", "Container", containerURLs, apiCL, null,
      SDCClassLoader.SYSTEM_CONTAINER_CLASSES_DEFAULT, SDCClassLoader.APPLICATION_CONTAINER_CLASSES_DEFAULT, false);

  }

  public static SDCClassLoader getStageClassLoader(String type, String name, List<URL> libURLs, ClassLoader apiCL) {
    return getStageClassLoader(type, name, libURLs, apiCL, false);
  }

  public static SDCClassLoader getStageClassLoader(String type, String name, List<URL> libURLs, ClassLoader apiCL,
      boolean isPrivate) {
    return new SDCClassLoader(type, name, libURLs, apiCL, PACKAGES_BLACKLIST_FOR_STAGE_LIBRARIES,
      SDCClassLoader.SYSTEM_STAGE_CLASSES_DEFAULT, SDCClassLoader.APPLICATION_STAGE_CLASSES_DEFAULT, isPrivate);

  }

  public SDCClassLoader duplicateStageClassLoader() {
    return getStageClassLoader(getType(), getName(), urls, parent, true);
  }

  public boolean isPrivate() {
    return isPrivate;
  }

  public String toString() {
    return String.format("SDCClassLoader[type=%s name=%s private=%b]", getType(), getName(), isPrivate);
  }

}