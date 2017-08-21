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

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Method;
import java.net.URL;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class BootstrapMain {
  private static final String PIPELINE_BOOTSTRAP_DEBUG_SYS_PROP = "streamsets.bootstrap.debug";
  public static final String PIPELINE_BOOTSTRAP_CLASSLOADER_SYS_PROP = "streamsets.classloader.debug";
  private static final String STREAMSETS_LIBRARIES_EXTRA_DIR_SYS_PROP = "STREAMSETS_LIBRARIES_EXTRA_DIR";

  private static final String MAIN_CLASS_OPTION = "-mainClass";
  private static final String API_CLASSPATH_OPTION = "-apiClasspath";
  private static final String CONTAINER_CLASSPATH_OPTION = "-containerClasspath";
  private static final String STREAMSETS_LIBRARIES_DIR_OPTION = "-streamsetsLibrariesDir";
  private static final String STREAMSETS_LIBRARIES_EXTRA_DIR_OPTION = "-streamsetsLibrariesExtraDir";
  private static final String USER_LIBRARIES_DIR_OPTION = "-userLibrariesDir";
  private static final String LIBS_COMMON_LIB_DIR_OPTION = "-libsCommonLibDir";
  private static final String CONFIG_DIR_OPTION = "-configDir";

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

  static final String SDC_CONFIG_FILE = "sdc.properties";

  static final String WHITE_LIST_FILE = "stagelibswhitelist.properties";
  static final String SYSTEM_LIBS_WHITE_LIST_KEY = "system.stagelibs.whitelist";
  static final String USER_LIBS_WHITE_LIST_KEY = "user.stagelibs.whitelist";
  static final String SYSTEM_LIBS_BLACK_LIST_KEY = "system.stagelibs.blacklist";
  static final String USER_LIBS_BLACK_LIST_KEY = "user.stagelibs.blacklist";
  static final String ALL_VALUES = "*";

  private static final String WHITE_LIST_PROPERTY_MISSING_MSG = "WhiteList property '%s' is missing in in file '%s'";
  private static final String WHITE_LIST_COULD_NOT_LOAD_FILE_MSG = "Could not load WhiteList file '%s': %s";

  private static final String DEBUG_MSG_PREFIX = "DEBUG: ";
  private static final String WARN_MSG_PREFIX = "WARN : ";

  private static final String PARSER_LIMIT = "parser.limit";

  private static final String DEBUG_MSG = DEBUG_MSG_PREFIX + "'%s' %s";
  public static final String WARN_MSG = WARN_MSG_PREFIX + "'%s' %s";

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
    String streamsetsLibrariesExtraDir = null;
    String userLibrariesDir = null;
    String configDir = null;
    String libsCommonLibDir = null;
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
      } else if (args[i].equals(STREAMSETS_LIBRARIES_EXTRA_DIR_OPTION)) {
        i++;
        if (i < args.length) {
          streamsetsLibrariesExtraDir = args[i];
        } else {
          throw new IllegalArgumentException(String.format(MISSING_ARG_MSG, STREAMSETS_LIBRARIES_EXTRA_DIR_OPTION));
        }
      } else if (args[i].equals(USER_LIBRARIES_DIR_OPTION)) {
        i++;
        if (i < args.length) {
          userLibrariesDir = args[i];
        } else {
          throw new IllegalArgumentException(String.format(MISSING_ARG_MSG, USER_LIBRARIES_DIR_OPTION));
        }
      } else if (args[i].equals(CONFIG_DIR_OPTION)) {
        i++;
        if (i < args.length) {
          configDir = args[i];
        } else {
          throw new IllegalArgumentException(String.format(MISSING_ARG_MSG, USER_LIBRARIES_DIR_OPTION));
        }
      } else if (args[i].equals(LIBS_COMMON_LIB_DIR_OPTION)) {
        i++;
        if (i < args.length) {
          libsCommonLibDir = args[i];
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
    if (configDir == null) {
      throw new IllegalArgumentException(String.format(OPTION_NOT_SPECIFIED_MSG, CONFIG_DIR_OPTION));
    }

    boolean debug = Boolean.getBoolean(PIPELINE_BOOTSTRAP_DEBUG_SYS_PROP);
    if (debug) {
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.println(String.format(DEBUG_MSG, CONFIG_DIR_OPTION, configDir));
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.println(String.format(DEBUG_MSG, MAIN_CLASS_OPTION, mainClass));
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.println(String.format(DEBUG_MSG, API_CLASSPATH_OPTION, apiClasspath));
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.println(String.format(DEBUG_MSG, CONTAINER_CLASSPATH_OPTION, containerClasspath));
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.println(String.format(DEBUG_MSG, STREAMSETS_LIBRARIES_DIR_OPTION, streamsetsLibrariesDir));
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.println(String.format(DEBUG_MSG, STREAMSETS_LIBRARIES_EXTRA_DIR_OPTION, streamsetsLibrariesExtraDir));
      System.out.println(DEBUG_MSG_PREFIX);
      System.out.println(String.format(DEBUG_MSG, USER_LIBRARIES_DIR_OPTION, userLibrariesDir));
    }

    // Extract classpath URLs for API, Container and Stage Libraries
    List<URL> apiUrls = getClasspathUrls(apiClasspath);
    List<URL> containerUrls = getClasspathUrls(containerClasspath);

    Set<String> systemStageLibs;
    Set<String> userStageLibs;
    if (isDeprecatedWhiteListConfiguration(configDir)) {
      System.out.println(String.format(
          WARN_MSG,
          "Using deprecated stage library whitelist configuration file",
          WHITE_LIST_FILE
      ));
      systemStageLibs = getWhiteList(configDir, SYSTEM_LIBS_WHITE_LIST_KEY);
      userStageLibs = getWhiteList(configDir, USER_LIBS_WHITE_LIST_KEY);
    } else {
      systemStageLibs = getSystemStageLibs(configDir);
      userStageLibs = getUserStageLibs(configDir);
    }

    if (debug) {
      String whiteListStr = (systemStageLibs == null) ? ALL_VALUES : systemStageLibs.toString();
      System.out.println(String.format(DEBUG_MSG, "System stage libs", whiteListStr));
      whiteListStr = (userStageLibs == null) ? ALL_VALUES : userStageLibs.toString();
      System.out.println(String.format(DEBUG_MSG, "User stage libs", whiteListStr));
    }

    Map<String, List<URL>> streamsetsLibsUrls = getStageLibrariesClasspaths(streamsetsLibrariesDir,
        streamsetsLibrariesExtraDir, systemStageLibs, libsCommonLibDir);
    Map<String, List<URL>> userLibsUrls = getStageLibrariesClasspaths(userLibrariesDir, null, systemStageLibs,
        libsCommonLibDir);

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

    setOverRunProperties(configDir);

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

  private static void setOverRunProperties(String configDir) {
    Properties config = readSdcConfiguration(configDir);

    if (config.containsKey(PARSER_LIMIT) && Integer.parseInt(config.getProperty(PARSER_LIMIT)) > 1024 * 1024) {
      System.setProperty("DataFactoryBuilder.OverRunLimit", config.getProperty(PARSER_LIMIT));
      System.setProperty("overrun.reader.read.limit", config.getProperty(PARSER_LIMIT));
    }
  }

  public static Set<String> getSystemStageLibs(String configDir) {
    return getStageLibs(configDir, SYSTEM_LIBS_WHITE_LIST_KEY, SYSTEM_LIBS_BLACK_LIST_KEY);
  }

  public static Set<String> getUserStageLibs(String configDir) {
    return getStageLibs(configDir, USER_LIBS_WHITE_LIST_KEY, USER_LIBS_BLACK_LIST_KEY);
  }

  public static Set<String> getStageLibs(String configDir, String whiteListKey, String blackListKey) {
    Set<String> stageLibs = null;
    if (isDeprecatedWhiteListConfiguration(configDir)) {
      System.out.println(String.format(
          WARN_MSG,
          "Using deprecated stage library whitelist configuration file",
          WHITE_LIST_FILE
      ));
      stageLibs = getWhiteList(configDir, whiteListKey);
    } else {
      Properties config = readSdcConfiguration(configDir);
      validateWhiteBlackList(config, whiteListKey, blackListKey);
      if (config.containsKey(whiteListKey)) {
        stageLibs = getList(config, whiteListKey, true);
      } else if (config.containsKey(blackListKey)) {
        stageLibs = getList(config, blackListKey, false);
      }
    }
    return stageLibs;
  }

  // deprecated as of SDC 1.2
  //
  // if whitelist is '*' set is NULL, else whitelist has the whitelisted values
  public static Set<String> getWhiteList(String configDir, String property) {
    Set<String> set = null;
    File whiteListFile = new File(configDir, WHITE_LIST_FILE).getAbsoluteFile();
    if (whiteListFile.exists()) {
      try (InputStream is = new FileInputStream(whiteListFile)) {
        Properties props = new Properties();
        props.load(is);
        String whiteList = props.getProperty(property);
        if (whiteList == null) {
          throw new IllegalArgumentException(String.format(WHITE_LIST_PROPERTY_MISSING_MSG, property, whiteListFile));
        }
        whiteList = whiteList.trim();
        if (!whiteList.equals(ALL_VALUES)) {
          set = new HashSet<>();
          for (String name : whiteList.split(",")) {
            name = name.trim();
            if (!name.isEmpty()) {
              set.add("+" + name.trim());
            }
          }
        }
      } catch (IOException ex) {
        throw new IllegalArgumentException(String.format(WHITE_LIST_COULD_NOT_LOAD_FILE_MSG,
                                                         whiteListFile, ex.toString()), ex);
      }
    }
    return set;
  }

  private static final String CANNOT_READ_SDC_CONFIG_FILE = "File '%s' cannot be read: %s";
  private static final String CANNOT_HAVE_WHITE_BLACK_LIST_MSG =
      "Configuration file '%s' cannot define both '%s' and '%s' properties";

  public static boolean isDeprecatedWhiteListConfiguration(String configDir) {
    return new File(configDir, WHITE_LIST_FILE).getAbsoluteFile().exists();
  }

  public static Properties readSdcConfiguration(String configDir) {
    File file = new File(configDir, SDC_CONFIG_FILE).getAbsoluteFile();
    try (InputStream is = new FileInputStream(file)) {
      Properties props = new Properties();
      props.load(is);
      return props;
    } catch (IOException ex) {
      throw new IllegalArgumentException(String.format(CANNOT_READ_SDC_CONFIG_FILE, file, ex.toString()));
    }
  }

  public static void validateWhiteBlackList(Properties props, String whiteList, String blackList) {
    if (props.containsKey(whiteList) && props.containsKey(blackList)) {
      throw new IllegalArgumentException(String.format(
          CANNOT_HAVE_WHITE_BLACK_LIST_MSG,
          SDC_CONFIG_FILE,
          whiteList,
          blackList
      ));
    }
  }

  public static Set<String> getList(Properties props, String property, boolean whitelist) {
    String prefix = (whitelist) ? "+" : "-";
    Set<String> set = null;
    String whiteList = props.getProperty(property, null);
    if (whiteList != null) {
      set = new HashSet<>();
      for (String name : whiteList.split(",")) {
        name = name.trim();
        if (!name.isEmpty()) {
          set.add(prefix + name.trim());
        }
      }
    }
    return set;
  }

  public static FileFilter createStageLibFilter(final Set<String> stageLibs) {
   return new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        boolean accept = false;
        if (pathname.isDirectory()) {
          if (stageLibs == null) {
            // stageLibs == NULL means ALL
            accept = true;
          } else {
            boolean isWhiteList = stageLibs.iterator().next().startsWith("+");
            if (isWhiteList && stageLibs.contains("+" + pathname.getName())) {
              accept = true;
            } else if (!isWhiteList && !stageLibs.contains("-" + pathname.getName())) {
              accept = true;
            }
          }
        }
        return accept;
      }
    };
  }

  // Visible for testing
  public static Map<String, List<URL>> getStageLibrariesClasspaths(String stageLibrariesDir, String librariesExtraDir,
      final Set<String> stageLibs, String libsCommonLibDir) throws Exception {
    Map<String, List<URL>> map = new LinkedHashMap<String, List<URL>>();

    File baseDir = new File(stageLibrariesDir).getAbsoluteFile();
    if (baseDir.exists()) {
      File[] libDirs = baseDir.listFiles(createStageLibFilter(stageLibs));
      StringBuilder commonLibJars = new StringBuilder();
      if (libsCommonLibDir != null) {
        commonLibJars.append(new File(libsCommonLibDir).getAbsolutePath()).append(FILE_SEPARATOR).append(JARS_WILDCARD).
            append(CLASSPATH_SEPARATOR);
      }
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
        sb.append(commonLibJars);
        sb.append(jarsDir.getAbsolutePath()).append(FILE_SEPARATOR).append(JARS_WILDCARD);

        // add extralibs if avail
        if (librariesExtraDir != null) {
          System.setProperty(STREAMSETS_LIBRARIES_EXTRA_DIR_SYS_PROP, librariesExtraDir);
          File libExtraDir = new File(librariesExtraDir, libDir.getName());
          if (libExtraDir.exists()) {
            File extraJarsDir = new File(libExtraDir, STAGE_LIB_JARS_DIR);
            if (extraJarsDir.exists()) {
              sb.append(CLASSPATH_SEPARATOR).append(extraJarsDir.getAbsolutePath()).append(FILE_SEPARATOR).
                  append(JARS_WILDCARD);
            }
            File extraEtc = new File(libExtraDir, STAGE_LIB_CONF_DIR);
            if (extraEtc.exists()) {
              sb.append(CLASSPATH_SEPARATOR).append(extraEtc.getAbsolutePath());
            }
          }
        }

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
