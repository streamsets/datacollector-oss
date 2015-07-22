/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline;

import java.io.File;
import java.io.FileFilter;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Method;
import java.net.URL;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BootstrapMain {
  private static final String PIPELINE_BOOTSTRAP_DEBUG_SYS_PROP = "streamsets.bootstrap.debug";
  private static final String PIPELINE_BOOTSTRAP_CLASSLOADER_SYS_PROP = "streamsets.classloader.debug";

  private static final String MAIN_CLASS_OPTION = "-mainClass";
  private static final String API_CLASSPATH_OPTION = "-apiClasspath";
  private static final String CONTAINER_CLASSPATH_OPTION = "-containerClasspath";
  private static final String STREAMSETS_LIBRARIES_DIR_OPTION = "-streamsetsLibrariesDir";
  private static final String USER_LIBRARIES_DIR_OPTION = "-userLibrariesDir";

  private static final String SET_CONTEXT_METHOD = "setContext";
  private static final String MAIN_METHOD = "main";

  private static final String STAGE_LIB_JARS_DIR = "lib";
  private static final String STAGE_LIB_CONF_DIR = "etc";

  private static final String JARS_WILDCARD = "*.jar";
  public static final String FILE_SEPARATOR = System.getProperty("file.separator");
  public static final String CLASSPATH_SEPARATOR = System.getProperty("path.separator");

  private static final String MISSING_ARG_MSG = "Missing argument for '%s'";
  private static final String INVALID_OPTION_ARG_MSG = "Invalid option or argument '%s'";
  private static final String OPTION_NOT_SPECIFIED_MSG = "Option not specified '%s <ARG>'";
  private static final String MISSING_STAGE_LIB_JARS_DIR_MSG = "Invalid library '%s', missing lib directory";
  private static final String MISSING_STAGE_LIBRARIES_DIR_MSG = "Stage libraries directory '%s' does not exist";
  private static final String CLASSPATH_DIR_DOES_NOT_EXIST_MSG = "Classpath directory '%s' does not exist";
  private static final String CLASSPATH_PATH_S_IS_NOT_A_DIR_MSG = "Specified Classpath path '%s' is not a directory";

  private static final String DEBUG_MSG_PREFIX = "DEBUG: ";

  private static final String DEBUG_MSG = DEBUG_MSG_PREFIX + "'%s' %s";

  private static Instrumentation instrumentation;

  public static Instrumentation getInstrumentation() {
    return instrumentation;
  }
  /**
   * Visible due to JVM requirements only
   */
  public static void premain(String args, Instrumentation instrumentation) {
    if (BootstrapMain.instrumentation == null) {
      BootstrapMain.instrumentation = instrumentation;
    } else {
      throw new IllegalStateException("Premain method cannot be called twice (" + BootstrapMain.instrumentation + ")");
    }
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    try {
      System.getProperty("test.to.ensure.security.is.configured.correctly");
    } catch (AccessControlException e) {
      String msg = "Error: Security is enabled but sdc policy file is misconfigured";
      throw new IllegalArgumentException(msg, e);
    }
    SDCClassLoader.setDebug(Boolean.getBoolean(PIPELINE_BOOTSTRAP_CLASSLOADER_SYS_PROP));

    String mainClass = null;
    String apiClasspath = null;
    String containerClasspath = null;
    String streamsetsLibrariesDir = null;
    String userLibrariesDir = null;
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
      } else if (args[i].equals(STREAMSETS_LIBRARIES_DIR_OPTION)) {
        i++;
        if (i < args.length) {
          streamsetsLibrariesDir = args[i];
        } else {
          throw new IllegalArgumentException(String.format(MISSING_ARG_MSG, STREAMSETS_LIBRARIES_DIR_OPTION));
        }
      } else if (args[i].equals(USER_LIBRARIES_DIR_OPTION)) {
        i++;
        if (i < args.length) {
          userLibrariesDir = args[i];
        } else {
          throw new IllegalArgumentException(String.format(MISSING_ARG_MSG, USER_LIBRARIES_DIR_OPTION));
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
    if (streamsetsLibrariesDir == null) {
      throw new IllegalArgumentException(String.format(OPTION_NOT_SPECIFIED_MSG, STREAMSETS_LIBRARIES_DIR_OPTION));
    }
    if (userLibrariesDir == null) {
      throw new IllegalArgumentException(String.format(OPTION_NOT_SPECIFIED_MSG, USER_LIBRARIES_DIR_OPTION));
    }

    boolean debug = Boolean.getBoolean(PIPELINE_BOOTSTRAP_DEBUG_SYS_PROP);
    if (debug) {
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.println(String.format(DEBUG_MSG, MAIN_CLASS_OPTION, mainClass));
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.println(String.format(DEBUG_MSG, API_CLASSPATH_OPTION, apiClasspath));
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.println(String.format(DEBUG_MSG, CONTAINER_CLASSPATH_OPTION, containerClasspath));
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.println(String.format(DEBUG_MSG, STREAMSETS_LIBRARIES_DIR_OPTION, streamsetsLibrariesDir));
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.println(String.format(DEBUG_MSG, USER_LIBRARIES_DIR_OPTION, userLibrariesDir));
    }

    // Extract classpath URLs for API, Container and Stage Libraries
    List<URL> apiUrls = getClasspathUrls(apiClasspath);
    List<URL> containerUrls = getClasspathUrls(containerClasspath);
    Map<String, List<URL>> streamsetsLibsUrls = getStageLibrariesClasspaths(streamsetsLibrariesDir);
    Map<String, List<URL>> userLibsUrls = getStageLibrariesClasspaths(userLibrariesDir);

    if (debug) {
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.println(String.format(DEBUG_MSG, "API classpath            : ", apiUrls));
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.println(String.format(DEBUG_MSG, "Container classpath      :", containerUrls));
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.println(String.format(DEBUG_MSG, "StreamSets libraries classpath:", streamsetsLibsUrls));
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.println(String.format(DEBUG_MSG, "User libraries classpath:", userLibsUrls));
      System.out.println(DEBUG_MSG_PREFIX);
    }

    Map<String, List<URL>> libsUrls = new LinkedHashMap<>();
    libsUrls.putAll(streamsetsLibsUrls);
    libsUrls.putAll(userLibsUrls);


    // Create all ClassLoaders
    SDCClassLoader apiCL = SDCClassLoader.getAPIClassLoader(apiUrls, ClassLoader.getSystemClassLoader());
    if (debug) {
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.print(apiCL.dumpClasspath(DEBUG_MSG_PREFIX));
      System.out.println(DEBUG_MSG_PREFIX);
    }
    SDCClassLoader containerCL = SDCClassLoader.getContainerCLassLoader(containerUrls, apiCL);
    if (debug) {
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.print(containerCL.dumpClasspath(DEBUG_MSG_PREFIX));
      System.out.println(DEBUG_MSG_PREFIX);
    }
    List<ClassLoader> stageLibrariesCLs = new ArrayList<>();
    for (Map.Entry<String,List<URL>> entry : libsUrls.entrySet()) {
      String[] parts = entry.getKey().split(FILE_SEPARATOR);
      if (parts.length != 2) {
        String msg = "Invalid library name: " + entry.getKey();
        throw new IllegalStateException(msg);
      }
      String type = parts[0];
      String name = parts[1];
      SDCClassLoader cl = SDCClassLoader.getStageClassLoader(type, name, entry.getValue(), apiCL);
      if (debug) {
        System.out.println(DEBUG_MSG_PREFIX);
        System.out.print(cl.dumpClasspath(DEBUG_MSG_PREFIX));
        System.out.println(DEBUG_MSG_PREFIX);
      }
      stageLibrariesCLs.add(cl);
    }

    // Bootstrap container
    Thread.currentThread().setContextClassLoader(containerCL);
    Class klass = containerCL.loadClass(mainClass);
    Method method = klass.getMethod(SET_CONTEXT_METHOD, ClassLoader.class, ClassLoader.class, List.class,
      Instrumentation.class);
    method.invoke(null, apiCL, containerCL, stageLibrariesCLs, instrumentation);
    method = klass.getMethod(MAIN_METHOD, String[].class);
    method.invoke(null, new Object[]{new String[]{}});
  }

  // Visible for testing
  public static Map<String, List<URL>> getStageLibrariesClasspaths(String stageLibrariesDir) throws Exception {
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
          throw new IllegalArgumentException(String.format(MISSING_STAGE_LIB_JARS_DIR_MSG, libDir));
        }
        StringBuilder sb = new StringBuilder();
        if (etc.exists()) {
          sb.append(etc.getAbsolutePath()).append(FILE_SEPARATOR).append(CLASSPATH_SEPARATOR);
        }
        sb.append(jarsDir.getAbsolutePath()).append(FILE_SEPARATOR).append(JARS_WILDCARD);
        map.put(libDir.getParentFile().getName() + FILE_SEPARATOR + libDir.getName(), getClasspathUrls(sb.toString()));
      }
    } else {
      throw new IllegalArgumentException(String.format(MISSING_STAGE_LIBRARIES_DIR_MSG, baseDir));
    }
    return map;
  }

  // Visible for testing
  public static List<URL> getClasspathUrls(String classPath)
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
            throw new IllegalArgumentException(String.format(CLASSPATH_DIR_DOES_NOT_EXIST_MSG, f));
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
              throw new IllegalArgumentException(String.format(CLASSPATH_PATH_S_IS_NOT_A_DIR_MSG, f));
            }
          } else {
            throw new IllegalArgumentException(String.format(CLASSPATH_DIR_DOES_NOT_EXIST_MSG, f));
          }
        }
      }
    }
    return urls;
  }

  // for test coverage purposes only
  BootstrapMain() {
  }

}
