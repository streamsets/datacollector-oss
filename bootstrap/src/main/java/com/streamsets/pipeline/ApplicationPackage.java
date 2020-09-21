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

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ApplicationPackage {
  private static final String MACOS_JAVA_EXTENSIONS_DIR = "/System/Library/Java/Extensions/";
  private static final String JAVA_HOME;
  private static final String STREAMSETS_PACKAGE = "com.streamsets";
  private static final String CLASS_FILE_SUFFIX = ".class";
  private static final String JAR_FILE_SUFFIX = ".jar";
  static {
    String javaHome = System.getProperty("java.home");
    if (javaHome.endsWith("jre")) {
      javaHome = javaHome.substring(0, javaHome.lastIndexOf("jre"));
    }
    JAVA_HOME = javaHome;
    if(!ApplicationPackage.class.getPackage().getName().startsWith(STREAMSETS_PACKAGE)) {
      throw new RuntimeException("Refactor occurred without changing StreamSets package name");
    }
  }
  private static final Map<ClassLoader, ApplicationPackage> instances = new HashMap<>();

  private final SortedSet<String> packages;
  private final int lengthOfSmallestPackage;
  private int operationCount;

  public ApplicationPackage(ClassLoader cl) {
    this(findApplicationPackageNames(cl));
  }

  public ApplicationPackage(SortedSet<String> packages) {
    this.packages = packages;
    this.operationCount = 0;
    int lengthOfSmallestPackage = Integer.MAX_VALUE;
    for (String packageName : packages) {
      lengthOfSmallestPackage = Math.min(packageName.length(), lengthOfSmallestPackage);
    }
    this.lengthOfSmallestPackage = lengthOfSmallestPackage;
  }

  @Override
  public String toString() {
    return "ApplicationPackage{" +
      "packages=" + packages +
      ", lengthOfSmallestPackage=" + lengthOfSmallestPackage +
      '}';
  }

  public boolean isApplication(String packageName) {
    if (packageName.isEmpty()) {
      return false;
    }
    /*
     * packages contains a list of java packages and if the argument
     * resides in any of those packages, we want to return true that
     * this package should be considered an application package.
     *
     * One option is to perform a linear search but that is of course
     * would be very slow. As such packages is stored as a sorted set
     * which allows us to use the sort order of the package names
     * to reduce the number of operations.
     *
     * Assume we have packages a. b. c. if searching for b.1. the ideal
     * case is to search the set sub-set of (b.) and to find this sub-set
     * we need two bounds. The lower bound is easy, it's b.1 while the
     * upper bound is also trivial in this case either b or b.
     *
     * In general the upper bounds should be the largest portion of the
     * needle which excludes no possible matches. For example assume we
     * have packages org.apache.hadoop. and org.apache.xerces. and the need
     * is org.apache.hadoop.hdfs., for correctness the longest upper bound is
     * org.apache.hadoop.
     */
    String canonicalName = ClassLoaderUtil.canonicalizeClassOrResource(packageName);
    String from;
    if (canonicalName.length() > lengthOfSmallestPackage) {
      from = canonicalName.substring(0, lengthOfSmallestPackage - 1);
    } else if (lengthOfSmallestPackage == Integer.MAX_VALUE) {
      return false;
    } else {
      from = canonicalName.substring(0, 1);
    }
    for (String otherPackageName : packages.subSet(from, canonicalName)) {
      operationCount++;
      if (canonicalName.startsWith(otherPackageName)) {
        return true;
      }
    }
    return false;
  }

  public int getOperationCount() {
    return operationCount;
  }

  public static synchronized ApplicationPackage get(ClassLoader cl) {
    ApplicationPackage result = instances.get(cl);
    if (result == null) {
      result = new ApplicationPackage(cl);
      instances.put(cl, result);
    }
    return result;
  }

  /**
   * Finds package names associated with a class loader which which are not JVM level
   * packages. Anything inside java home are specifically excluded as well as some
   * OS specific install locations (MacOS).
   */
  private static SortedSet<String> findApplicationPackageNames(ClassLoader cl) {
    SortedSet<String> packages = new TreeSet<>();
    if (cl instanceof URLClassLoader) {
      // JDK 8 Case
      while (cl != null) {
        if (cl instanceof URLClassLoader) {
          for (URL url : ((URLClassLoader)cl).getURLs()) {
            String path = url.getPath();
            if (!path.startsWith(JAVA_HOME) && !path.startsWith(MACOS_JAVA_EXTENSIONS_DIR) &&
                path.endsWith(JAR_FILE_SUFFIX)) {
              try {
                try (ZipInputStream zip = new ZipInputStream(url.openStream())) {
                  filterPackageNames(packages, zip);
                }
              } catch (IOException unlikely) {
                // since these are local URL we will likely only
                // hit this if there is a corrupt jar in the classpath
                // which we will ignore
                if (SDCClassLoader.isDebug()) {
                  System.err.println("Error opening '" + url + "' : " + unlikely);
                  unlikely.printStackTrace();
                }
              }
            }
          }
        }
        cl = cl.getParent();
      }
    } else {
      // JDK 9+ case
      // Java 9 and the module system improved the platform’s class loading strategy, which is implemented in a
      // new type and in Java 11 the application class loader is of that type.
      // https://blog.codefx.org/java/java-11-migration-guide/#Casting-To-URL-Class-Loader
      String pathSeparator = System.getProperty("path.separator");
      String[] classPathEntries = System.getProperty("java.class.path").split(pathSeparator);

      for (String path: classPathEntries) {
        if (!path.startsWith(JAVA_HOME) && !path.startsWith(MACOS_JAVA_EXTENSIONS_DIR) &&
            path.endsWith(JAR_FILE_SUFFIX)) {
          try {
            try (ZipInputStream zip = new ZipInputStream(new FileInputStream(path))) {
              filterPackageNames(packages, zip);
            }
          } catch (IOException unlikely) {
            // since these are local URL we will likely only
            // hit this if there is a corrupt jar in the classpath
            // which we will ignore
            if (SDCClassLoader.isDebug()) {
              System.err.println("Error opening '" + path + "' : " + unlikely);
              unlikely.printStackTrace();
            }
          }
        }
      }
    }

    SystemPackage systemPackage = new SystemPackage(SDCClassLoader.SYSTEM_API_CHILDREN_CLASSES);
    packages.removeIf(systemPackage::isSystem);
    removeLogicalDuplicates(packages);
    return packages;
  }

  private static void filterPackageNames(SortedSet<String> packages, ZipInputStream zip) throws IOException {
    for (ZipEntry entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
      if (!entry.isDirectory() && entry.getName().endsWith(CLASS_FILE_SUFFIX)) {
        // This ZipEntry represents a class. Now, what class does it represent?
        String className = entry.getName().replace('/', '.'); // including ".class"
        className = className.substring(0, className.length() - CLASS_FILE_SUFFIX.length());
        if (className.contains(".") && !className.startsWith(STREAMSETS_PACKAGE)) {
          // must end with a . as we don't want o.a.h matching o.a.ha
          packages.add(className.substring(0, className.lastIndexOf('.')) + ".");
        }
      }
    }
  }

  /**
   * Traverses sorted list of packages and removes logical duplicates. For example
   * if the set contains akka., akka.io., and akka.util. only akka. will remain.
   * Note that if the set contains only akka.io. and akka.util. both will remain.
   * Otherwise all of the org.apache. would devolve to org.
   */
  static void removeLogicalDuplicates(SortedSet<String> packages) {
    Iterator<String> iterator = packages.iterator();
    if (!iterator.hasNext()) {
      return;
    }
    String last = iterator.next();
    while (iterator.hasNext()) {
      String current = iterator.next();
      if (current.startsWith(last)) {
        iterator.remove();
      } else {
        last = current;
      }
    }
  }
}
