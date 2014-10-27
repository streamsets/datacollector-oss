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
package com.streamsets.pipeline;

import java.io.File;
import java.io.FileFilter;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BootstrapMain {

  private static final boolean DEBUG = Boolean.getBoolean("pipeline.bootstrap.debug");

  private static final String[] PACKAGES_BLACKLIST_FOR_STAGE_LIBRARIES = {
      "com.streamsets.pipeline.api.",
      "com.streamsets.pipeline.container",
      "com.codehale.metrics.",
      "org.slf4j.",
      "org.apache.log4j."
  };

  private static final String MAIN_CLASS_OPTION = "-mainClass";
  private static final String API_CLASSPATH_OPTION = "-apiClasspath";
  private static final String CONTAINER_CLASSPATH_OPTION = "-containerClasspath";
  private static final String STAGE_LIBRARIES_DIR_OPTION = "-stageLibrariesDir";

  private static final String SET_CLASS_LOADERS_METHOD = "setClassLoaders";
  private static final String MAIN_METHOD = "main";

  private static final String STAGE_LIB_JARS_DIR = "lib";
  private static final String STAGE_LIB_CONF_DIR = "etc";

  private static final String JARS_WILDCARD = "*.jar";
  private static final String FILE_SEPARATOR = System.getProperty("file.separator");
  private static final String CLASSPATH_SEPARATOR = System.getProperty("path.separator");

  private static final String MISSING_ARG_MSG = "Missing argument for '%s'";
  private static final String INVALID_OPTION_ARG_MSG = "Invalid option or argument '%s'";
  private static final String OPTION_NOT_SPECIFIED_MSG = "Option not specified '%s <ARG>'";
  private static final String MISSING_STAGE_LIB_JARS_DIR_MSG = "Invalid library '%s', missing lib directory";
  private static final String MISSING_STAGE_LIBRARIES_DIR_MSG = "Stage libraries directory '%s' does not exist";
  private static final String CLASSPATH_DIR_DOES_NOT_EXIST_MSG = "Classpath directory '%s' does not exist";
  private static final String CLASSPATH_PATH_S_IS_NOT_A_DIR_MSG = "Specified Classpath path '%s' is not a directory";

  private static final String DEBUG_MSG = "DEBUG: '%s' %s";

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    String mainClass = null;
    String apiClasspath = null;
    String containerClasspath = null;
    String stageLibrariesDir = null;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals(MAIN_CLASS_OPTION)) {
        i++;
        if (i < args.length) {
          mainClass = args[i];
        } else {
          throw new IllegalArgumentException(String.format(MISSING_ARG_MSG, MAIN_CLASS_OPTION));
        }

      } else if (args[i].equals(API_CLASSPATH_OPTION)) {
        i++;
        if (i < args.length) {
          apiClasspath = args[i];
        } else {
          throw new IllegalArgumentException(String.format(MISSING_ARG_MSG, API_CLASSPATH_OPTION));
        }
      } else if (args[i].equals(CONTAINER_CLASSPATH_OPTION)) {
        i++;
        if (i < args.length) {
          containerClasspath = args[i];
        } else {
          throw new IllegalArgumentException(String.format(MISSING_ARG_MSG, CONTAINER_CLASSPATH_OPTION));
        }
      } else if (args[i].equals(STAGE_LIBRARIES_DIR_OPTION)) {
        i++;
        if (i < args.length) {
          stageLibrariesDir = args[i];
        } else {
          throw new IllegalArgumentException(String.format(MISSING_ARG_MSG, STAGE_LIBRARIES_DIR_OPTION));
        }
      } else {
        throw new IllegalArgumentException(String.format(INVALID_OPTION_ARG_MSG, args[i]));
      }
    }
    if (mainClass == null) {
      throw new IllegalArgumentException(String.format(OPTION_NOT_SPECIFIED_MSG, MAIN_CLASS_OPTION));
    }
    if (apiClasspath == null) {
      throw new IllegalArgumentException(String.format(OPTION_NOT_SPECIFIED_MSG, API_CLASSPATH_OPTION));
    }
    if (containerClasspath == null) {
      throw new IllegalArgumentException(String.format(OPTION_NOT_SPECIFIED_MSG, CONTAINER_CLASSPATH_OPTION));
    }
    if (stageLibrariesDir == null) {
      throw new IllegalArgumentException(String.format(OPTION_NOT_SPECIFIED_MSG, STAGE_LIBRARIES_DIR_OPTION));
    }

    if (DEBUG) {
      System.out.println();
      System.out.println(String.format(DEBUG_MSG, MAIN_CLASS_OPTION, mainClass));
      System.out.println();
      System.out.println(String.format(DEBUG_MSG, API_CLASSPATH_OPTION, apiClasspath));
      System.out.println();
      System.out.println(String.format(DEBUG_MSG, CONTAINER_CLASSPATH_OPTION, containerClasspath));
      System.out.println();
      System.out.println(String.format(DEBUG_MSG, STAGE_LIBRARIES_DIR_OPTION, stageLibrariesDir));
    }

    // Extract classpath URLs for API, Container and Stage Libraries
    List<URL> apiUrls = getClasspathUrls(apiClasspath);
    List<URL> containerUrls = getClasspathUrls(containerClasspath);
    Map<String, List<URL>> stageLibsUrls = getStageLibrariesClasspaths(stageLibrariesDir);

    if (DEBUG) {
      System.out.println();
      System.out.println(String.format(DEBUG_MSG, "API classpath            : ", apiUrls));
      System.out.println();
      System.out.println(String.format(DEBUG_MSG, "Container classpath      :", containerUrls));
      System.out.println();
      System.out.println(String.format(DEBUG_MSG, "Stage libraries classpath:", stageLibsUrls));
      System.out.println();
    }

    // Create all ClassLoaders
    ClassLoader apiCL = new BlackListURLClassLoader(apiUrls, ClassLoader.getSystemClassLoader(), null);
    ClassLoader containerCL = new BlackListURLClassLoader(containerUrls, apiCL, null);
    List<ClassLoader> stageLibrariesCLs = new ArrayList<ClassLoader>();
    for (List<URL> stageLibUrls : stageLibsUrls.values()) {
      stageLibrariesCLs.add(new BlackListURLClassLoader(stageLibUrls, apiCL, PACKAGES_BLACKLIST_FOR_STAGE_LIBRARIES));
    }

    // Bootstrap container
    Thread.currentThread().setContextClassLoader(containerCL);
    Class klass = containerCL.loadClass(mainClass);
    Method method = klass.getMethod(SET_CLASS_LOADERS_METHOD, ClassLoader.class, ClassLoader.class, List.class);
    method.invoke(null, apiCL, containerCL, stageLibrariesCLs);
    method = klass.getMethod(MAIN_METHOD);
    method.invoke(null);
  }

  // Visible for testing
  static Map<String, List<URL>> getStageLibrariesClasspaths(String stageLibrariesDir) throws Exception {
    Map<String, List<URL>> map = new LinkedHashMap<String, List<URL>>();

    File baseDir = new File(stageLibrariesDir).getAbsoluteFile();
    if (baseDir.exists()) {
      File[] libDirs = baseDir.listFiles(new FileFilter() {
        @Override
        public boolean accept(File pathname) {
          return pathname.isDirectory();
        }
      });
      for (File libDir : libDirs) {
        File jarsDir = new File(libDir, STAGE_LIB_JARS_DIR);
        File etc = new File(libDir, STAGE_LIB_CONF_DIR);
        if (!jarsDir.exists()) {
          throw new RuntimeException(String.format(MISSING_STAGE_LIB_JARS_DIR_MSG, libDir));
        }
        StringBuilder sb = new StringBuilder();
        if (etc.exists()) {
          sb.append(etc.getAbsolutePath()).append(FILE_SEPARATOR).append(CLASSPATH_SEPARATOR);
        }
        sb.append(jarsDir.getAbsolutePath()).append(FILE_SEPARATOR).append(JARS_WILDCARD);
        map.put(libDir.getName(), getClasspathUrls(sb.toString()));
      }
    } else {
      throw new RuntimeException(String.format(MISSING_STAGE_LIBRARIES_DIR_MSG, baseDir));
    }
    return map;
  }

  // Visible for testing
  static List<URL> getClasspathUrls(String classPath)
      throws Exception {
    List<URL> urls = new ArrayList<URL>();
    for (String path : classPath.split(CLASSPATH_SEPARATOR)) {
      if (!path.isEmpty()) {
        if (path.toLowerCase().endsWith(JARS_WILDCARD)) {
          path = path.substring(0, path.length() - JARS_WILDCARD.length());
          File f = new File(path).getAbsoluteFile();
          if (f.exists()) {
            File[] jars = f.listFiles(new FileFilter() {
              @Override
              public boolean accept(File pathname) {
                return pathname.getName().toLowerCase().endsWith(JARS_WILDCARD.substring(1));
              }
            });
            for (File jar : jars) {
              urls.add(jar.toURI().toURL());
            }
          } else {
            throw new RuntimeException(String.format(CLASSPATH_DIR_DOES_NOT_EXIST_MSG, f));
          }
        } else {
          if (!path.endsWith(FILE_SEPARATOR)) {
            path = path + FILE_SEPARATOR;
          }
          File f = new File(path).getAbsoluteFile();
          if (f.exists()) {
            if (f.isDirectory()) {
              urls.add(f.toURI().toURL());
            } else {
              throw new RuntimeException(String.format(CLASSPATH_PATH_S_IS_NOT_A_DIR_MSG, f));
            }
          } else {
            throw new RuntimeException(String.format(CLASSPATH_DIR_DOES_NOT_EXIST_MSG, f));
          }
        }
      }
    }
    return urls;
  }

}
