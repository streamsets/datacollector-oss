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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
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
   * if you update this, you must also update api-children-classloader.properties
   */
  private static final String[] PACKAGES_BLACKLIST_FOR_STAGE_LIBRARIES = {
      "com.streamsets.pipeline.api.",
      "com.streamsets.pipeline.container.",
      "org.slf4j.",
      "org.apache.log4j."
  };

  /**
   * Default value of the system classes if the user did not override them.
   * JDK classes, hadoop classes and resources, and some select third-party
   * classes are considered system classes, and are not loaded by the
   * application classloader.
   */
  static final List<String> SYSTEM_API_CLASSES;
  static final List<String> SYSTEM_API_CHILDREN_CLASSES;
  private static String API = "api";
  private static String API_CHILDREN = "api-children";
  private static final String[] CLASSLOADER_TYPES = new String[] {
    API, API_CHILDREN
  };

  private static final String SYSTEM_CLASSES_DEFAULT_KEY =
    "system.classes.default";

  private static boolean debug = false;

  public static void setDebug(boolean debug) {
    SDCClassLoader.debug = debug;
  }

  public static boolean isDebug() {
    return debug;
  }

  static {
    Map<String, String> systemClassesDefaultsMap = new HashMap<>();
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
      } catch (IOException e) {
        throw new ExceptionInInitializerError(e);
      }
    }
    SYSTEM_API_CLASSES = Collections.unmodifiableList(Arrays.asList(ClassLoaderUtil.getTrimmedStrings(
      ClassLoaderUtil.checkNotNull(systemClassesDefaultsMap.get(API), API))));
    List<String> apiChildren = new ArrayList<>(Arrays.asList(ClassLoaderUtil.getTrimmedStrings(
      ClassLoaderUtil.checkNotNull(systemClassesDefaultsMap.get(API_CHILDREN), API_CHILDREN))));
    apiChildren.addAll(SYSTEM_API_CLASSES);
    SYSTEM_API_CHILDREN_CLASSES = Collections.unmodifiableList(apiChildren);
  }

  private final ClassLoader parent;
  private final boolean parentIsAPIClassLoader;
  private final SystemPackage systemPackage;
  private final boolean isPrivate;
  private final ApplicationPackage applicationPackage;

  public SDCClassLoader(String type, String name, List<URL> urls, ClassLoader parent, String[] blacklistedPackages,
      SystemPackage systemPackage, ApplicationPackage applicationPackage,
      boolean isPrivate, boolean parentIsAPIClassLoader, boolean isStageLibClassLoader) {
    super(type, name, getOrderedURLsForClassLoader(urls, isStageLibClassLoader, name), parent, blacklistedPackages);
    if (debug) {
      System.err.println(getClass().getSimpleName() + " " + getName() + ": urls: " + Arrays.toString(urls.toArray()));
      System.err.println(getClass().getSimpleName() + " " + getName() + ": system classes: " + systemPackage);
    }
    this.parent = parent;
    this.parentIsAPIClassLoader = parentIsAPIClassLoader;
    if (parent == null) {
      throw new IllegalArgumentException("No parent classloader!");
    }
    if (debug) {
      System.err.println(getClass().getSimpleName() + " " + getName() + ": parent classloader: " + parent);
    }
    if (systemPackage == null) {
      throw new IllegalArgumentException("System classes cannot be null");
    }
    // if the caller-specified system classes are null or empty, use the default
    this.systemPackage = systemPackage;
    if(debug) {
      System.err.println(getClass().getSimpleName() + " " + getName() + ": system classes: " + this.systemPackage);
    }
    this.applicationPackage = applicationPackage;
    this.isPrivate = isPrivate;
    if(debug) {
      System.err.println(getClass().getSimpleName() + " " + getName() + ": application packages: " + this.applicationPackage);
    }
  }

  /**
   * Arranges the urls in the following order:
   * <ul>
   *   <li>stage lib jars</li>
   *   <li>protolib jars</li>
   *   <li>non protolib jars</li>
   * </ul>
   *
   * @param stageLibName
   * @param urls
   * @return
   */
  static List<URL> bringStageAndProtoLibsToFront(String stageLibName, List<URL> urls) {
    List<URL> otherJars = new ArrayList<>();
    List<URL> protolibJars = new ArrayList<>();
    List<URL> stageLibjars = new ArrayList<>();
    for (URL url : urls) {
      String str = url.toExternalForm();
      if (str.endsWith(".jar")) {
        int nameIdx = str.lastIndexOf("/");
        if (nameIdx > -1) {
          String jarName = str.substring(nameIdx + 1);
          if (jarName.contains("-protolib-")) {
            // adding only protolib jars
            protolibJars.add(url);
          } else if (jarName.contains(stageLibName)) {
            stageLibjars.add(url);
          } else {
            otherJars.add(url);
          }
        } else {
          otherJars.add(url);
        }
      } else {
        otherJars.add(url);
      }
    }
    List<URL> allJars = new ArrayList<>();
    if (stageLibjars.size() != 1) {
      throw new ExceptionInInitializerError("Expected exactly 1 stage lib jar but found " + stageLibjars.size() +
          " with name " + stageLibName);
    }
    allJars.addAll(stageLibjars);
    allJars.addAll(protolibJars);
    allJars.addAll(otherJars);
    return allJars;
  }

  @Override
  public URL getResource(String name) {
    URL url = null;
    boolean isSystemPackage = systemPackage.isSystem(name);
    if (!isSystemPackage) {
      url = findResource(name);
      if (url == null && name.startsWith("/")) {
        if (debug) {
          System.err.println(getClass().getSimpleName() + " " + getName() + ": Remove leading / off " + name);
        }
        url = findResource(name.substring(1));
      }
    }

    if (url == null && (isSystemPackage || !applicationPackage.isApplication(name))) {
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
    if (!systemPackage.isSystem(name)) {
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
      if (applicationPackage.isApplication(name)) {
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
    if (!systemPackage.isSystem(name)) {
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
      if (applicationPackage.isApplication(name)) {
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
    boolean isSystemPackage = systemPackage.isSystem(name);
    if (c == null && !isSystemPackage) {
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
    // try parent classloader in the following situations:
    // 1. Package has been marked system
    // 2. parent is the API classloader
    // under most circumstances we do not want to try the parent classloader
    // for application classes, however this is not true if the parent is the api
    // classloader since we load the api and codahale/dropwizard metrics from there
    // 3. Class is not an application class
    if (c == null && (isSystemPackage || parentIsAPIClassLoader || !applicationPackage.isApplication(name))) {
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

  public static SDCClassLoader getAPIClassLoader(List<URL> apiURLs, ClassLoader parent) {
    return new SDCClassLoader("api-lib", "API", apiURLs, parent, null,
      new SystemPackage(SYSTEM_API_CLASSES), ApplicationPackage.get(parent), false, false, false);
  }

  public static SDCClassLoader getContainerCLassLoader(List<URL> containerURLs, ClassLoader apiCL) {
    return new SDCClassLoader("container-lib", "Container", containerURLs, apiCL,
      null, new SystemPackage(SYSTEM_API_CHILDREN_CLASSES),
      ApplicationPackage.get(apiCL.getParent()), false, true, false);

  }

  public static SDCClassLoader getStageClassLoader(String type, String name, List<URL> libURLs, ClassLoader apiCL) {
    return getStageClassLoader(type, name, libURLs, apiCL, false);
  }

  public static SDCClassLoader getStageClassLoader(String type, String name, List<URL> libURLs, ClassLoader apiCL,
      boolean isPrivate) {
    return new SDCClassLoader(type, name, libURLs, apiCL, PACKAGES_BLACKLIST_FOR_STAGE_LIBRARIES,
      new SystemPackage(SYSTEM_API_CHILDREN_CLASSES), ApplicationPackage.get(apiCL.getParent()),
      isPrivate, true, true);
  }

  public SDCClassLoader duplicateStageClassLoader() {
    return getStageClassLoader(getType(), getName(), urls, parent, true);
  }

  private static List<URL> getOrderedURLsForClassLoader(
      List<URL> urls,
      boolean isStageLibClassLoader,
      String stageLibName
  ) {
    // only for stagelib classloaders, we force stagelib and protolib JARs to be first in the classpath, so they can
    // override classes from its dependencies. Usecase: Hadoop native compression codecs replacement
    return (isStageLibClassLoader) ? bringStageAndProtoLibsToFront(stageLibName, urls) : urls;
  }


  public boolean isPrivate() {
    return isPrivate;
  }

  public String toString() {
    return String.format("SDCClassLoader[type=%s name=%s private=%b]", getType(), getName(), isPrivate);
  }
}
