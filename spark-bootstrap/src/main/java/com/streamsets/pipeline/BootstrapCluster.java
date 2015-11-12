/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
import java.io.FileInputStream;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * This class is responsible for all activities which cross classloaders. At present
 * there are two use cases for this class:
 * <ol>
 *   <li>Bootstrapping an Executor which is started as part of a spark job</li>
 *   <li>Obtaining a reference on the dummy source which is used to feed a pipeline</li>
 * </ol>
 */
public class BootstrapCluster {
  /**
   * We might have to have a reset method for unit tests
   */
  private static volatile boolean initialized = false;
  private static Properties properties;
  private static ClassLoader apiCL;
  private static ClassLoader containerCL;
  private static ClassLoader sparkCL;
  private static List<ClassLoader> stageLibrariesCLs;
  private static String dataDir;

  public static synchronized Properties getProperties() throws Exception {
    initialize();
    return properties;
  }

  private static synchronized void initialize() throws Exception {
    if (initialized) {
      return;
    }
    initialized = true;
    boolean isTestingMode = Boolean.getBoolean("sdc.testing-mode");
    String libraryRoot;
    String etcRoot;
    String resourcesRoot;
    if (isTestingMode) {
      libraryRoot = (new File(System.getProperty("user.dir"), "target")).getAbsolutePath();
      etcRoot = (new File(System.getProperty("user.dir"), "target")).getAbsolutePath();
      resourcesRoot = (new File(System.getProperty("user.dir"), "target")).getAbsolutePath();
    } else if (System.getProperty("SDC_MESOS_BASE_DIR") == null){
      libraryRoot = (new File(System.getProperty("user.dir"), "libs.tar.gz")).getAbsolutePath();
      etcRoot = (new File(System.getProperty("user.dir") + "/etc.tar.gz/etc/")).getAbsolutePath();
      resourcesRoot = (new File(System.getProperty("user.dir") + "/resources.tar.gz/resources/")).getAbsolutePath();
    } else {
      String sdcMesosBaseDir = System.getProperty("SDC_MESOS_BASE_DIR");
      libraryRoot = new File(sdcMesosBaseDir, "libs").getAbsolutePath();
      etcRoot = new File(sdcMesosBaseDir, "etc").getAbsolutePath();
      resourcesRoot = new File(sdcMesosBaseDir, "resources").getAbsolutePath();
    }
    System.setProperty("sdc.transient-env", "true");
    System.setProperty("sdc.static-web.dir", (new File(libraryRoot, "sdc-static-web")).getAbsolutePath());
    System.setProperty("sdc.conf.dir", etcRoot);
    System.setProperty("sdc.resources.dir", resourcesRoot);
    File sdcProperties = new File(etcRoot, "sdc.properties");
    if (!sdcProperties.isFile()) {
      String msg = "SDC Properties file does not exist at expected location: " + sdcProperties;
      throw new IllegalStateException(msg);
    }
    properties = new Properties();
    properties.load(new FileInputStream(sdcProperties));
    File rootDataDir = new File(etcRoot, "data");
    dataDir = rootDataDir.getAbsolutePath();
    File basePipelineDir = new File(rootDataDir, "pipelines");
    String pipelineName = properties.getProperty("cluster.pipeline.name");
    if (pipelineName == null) {
      throw new IllegalStateException("Pipeline to be run cannot be null");
    }
    SDCClassLoader.setDebug(Boolean.getBoolean(BootstrapMain.PIPELINE_BOOTSTRAP_CLASSLOADER_SYS_PROP));

    List<URL> apiUrls;
    List<URL> containerUrls;
    Map<String, List<URL>> streamsetsLibsUrls;
    Map<String, List<URL>> userLibsUrls;
    if (isTestingMode) {
      apiUrls = new ArrayList<URL>();
      containerUrls = new ArrayList<URL>();
      streamsetsLibsUrls = new HashMap<String, List<URL>>();
      userLibsUrls = new HashMap<String, List<URL>>();
      // for now we pull in container in for testing mode
      streamsetsLibsUrls.put("streamsets-libs/streamsets-datacollector-spark-protolib",
        BootstrapMain.getClasspathUrls(System.getProperty("user.dir") + "/target/"));
    } else {
      apiUrls = BootstrapMain.getClasspathUrls(libraryRoot + "/api-lib/*.jar");
      containerUrls = BootstrapMain.getClasspathUrls(libraryRoot + "/container-lib/*.jar");

      Set<String> systemWhiteList = BootstrapMain.getWhiteList(etcRoot, BootstrapMain.SYSTEM_LIBS_KEY);
      Set<String> userWhiteList = BootstrapMain.getWhiteList(etcRoot, BootstrapMain.USER_LIBS_KEY);

      String libsCommonLibDir = libraryRoot + "/libs-common-lib";

      // in cluster mode, the library extra dir files from the master are collapsed on the library dir
      streamsetsLibsUrls = BootstrapMain.getStageLibrariesClasspaths(libraryRoot + "/streamsets-libs", null,
                                                                     systemWhiteList, libsCommonLibDir);
      userLibsUrls = BootstrapMain.getStageLibrariesClasspaths(libraryRoot + "/user-libs", null, userWhiteList,
                                                               libsCommonLibDir);
    }
    Map<String, List<URL>> libsUrls = new LinkedHashMap<String, List<URL>> ();
    libsUrls.putAll(streamsetsLibsUrls);
    libsUrls.putAll(userLibsUrls);
    ClassLoader parent = Thread.currentThread().getContextClassLoader();
    if (parent == null) {
      parent = ClassLoader.getSystemClassLoader();
    }
    apiCL = SDCClassLoader.getAPIClassLoader(apiUrls, parent);
    containerCL = SDCClassLoader.getContainerCLassLoader(containerUrls, apiCL);
    stageLibrariesCLs = new ArrayList<>();
    File pipelineDir = new File(basePipelineDir, escapedPipelineName(apiCL, pipelineName));
    File pipelineJsonFile = new File(pipelineDir, "pipeline.json");
    if (!pipelineJsonFile.isFile()) {
      String msg = "Pipeline JSON file does not exist at expected location: " + pipelineJsonFile;
      throw new IllegalStateException(msg);
    }
    String pipelineJson;
    try {
      pipelineJson = new String(Files.readAllBytes(Paths.get(pipelineJsonFile.toURI())), StandardCharsets.UTF_8);
    } catch (Exception ex) {
      String msg = "Error reading Pipeline JSON File at: " + pipelineJsonFile;
      throw new IllegalStateException(msg, ex);
    }

    String sparkLib = getSourceLibraryName(pipelineJson);
    if (sparkLib == null) {
      throw new IllegalStateException("Couldn't find the source library in pipeline file");
    }
    String lookupLib = "streamsets-libs" +"/" + sparkLib;
    System.err.println("\n Cluster lib is " + lookupLib);
    for (Map.Entry<String,List<URL>> entry : libsUrls.entrySet()) {
      String[] parts = entry.getKey().split(System.getProperty("file.separator"));
      if (parts.length != 2) {
        String msg = "Invalid library name: " + entry.getKey();
        throw new IllegalStateException(msg);
      }
      String type = parts[0];
      String name = parts[1];
      SDCClassLoader sdcClassLoader = SDCClassLoader.getStageClassLoader(type, name, entry.getValue(), apiCL);
      // TODO add spark, scala, etc to blacklist
      if (lookupLib.equals(entry.getKey())) {
        if (sparkCL != null) {
          throw new IllegalStateException("Found two classloaders for " + lookupLib);
        }
        sparkCL = sdcClassLoader;
      }
      stageLibrariesCLs.add(sdcClassLoader);
    }
    if (sparkCL == null) {
      throw new IllegalStateException("Could not find classloader for " + lookupLib);
    }
    try {
      Instrumentation instrumentation = BootstrapMain.getInstrumentation();
      if (instrumentation != null) {
        Method memoryUsageCollectorInitialize = Class.forName("com.streamsets.datacollector.memory.MemoryUsageCollector",
          true, containerCL).getMethod("initialize", Instrumentation.class);
        memoryUsageCollectorInitialize.invoke(null, instrumentation);
      }
    } catch (Exception ex) {
      String msg = "Error trying to initialize MemoryUsageCollector: " + ex;
      throw new IllegalStateException(msg, ex);
    }
    try {
      Class<?> runtimeModuleClz = Class.forName("com.streamsets.datacollector.main.RuntimeModule", true, containerCL);
      Method setStageLibraryClassLoadersMethod = runtimeModuleClz.getMethod("setStageLibraryClassLoaders", List.class);
      setStageLibraryClassLoadersMethod.invoke(null, stageLibrariesCLs);
    } catch (Exception ex) {
      String msg = "Error trying to bookstrap Spark while setting stage classloaders: " + ex;
      throw new IllegalStateException(msg, ex);
    }
  }

  private static String escapedPipelineName(ClassLoader apiCL, String name) {
    try {
      Class pipelineUtils = apiCL.loadClass("com.streamsets.pipeline.api.impl.PipelineUtils");
      Method escapedPipelineName = pipelineUtils.getMethod("escapedPipelineName", String.class);
      return (String) escapedPipelineName.invoke(null, name);
    } catch (Exception ex) {
      throw new RuntimeException("Error escaping pipeline name '" + name + "': " + ex, ex);
    }
  }

  /**
   * Obtaining a reference on the dummy source which is used to feed a pipeline<br/>
   * Direction: Stage -> Container
   * @param postBatchRunnable
   * @return a source object associated with the newly created pipeline
   * @throws Exception
   */
  public static /*Source*/ Object startPipeline(
                                                 Runnable postBatchRunnable) throws Exception {
    BootstrapCluster.initialize();
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(containerCL);
      Class embeddedPipelineFactoryClz = Class.forName("com.streamsets.datacollector.EmbeddedDataCollectorFactory", true,
        containerCL);
      Method createPipelineMethod = embeddedPipelineFactoryClz.getMethod("startPipeline", Runnable.class);
      return createPipelineMethod.invoke(null, postBatchRunnable);
    } catch (Exception ex) {
      String msg = "Error trying to create pipeline: " + ex;
      throw new IllegalStateException(msg, ex);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  private static String getSourceLibraryName(String pipelineJson) throws Exception {
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(containerCL);
      Class pipelineConfigurationUtil =
        Class.forName("com.streamsets.datacollector.util.PipelineConfigurationUtil", true, containerCL);
      Method createPipelineMethod = pipelineConfigurationUtil.getMethod("getSourceLibName", String.class);
      return (String) createPipelineMethod.invoke(null, pipelineJson);
    } catch (Exception ex) {
      String msg = "Error trying to retrieve library name from pipeline json: " + ex;
      throw new IllegalStateException(msg, ex);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  public static Object getClusterFunction(Integer id) throws Exception {
    BootstrapCluster.initialize();
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(sparkCL);
      return Class.forName("com.streamsets.pipeline.cluster.ClusterFunctionImpl", true,
        sparkCL).getMethod("create", Properties.class, Integer.class, String.class).invoke(null, properties, id, dataDir);
    } catch (Exception ex) {
      String msg = "Error trying to obtain ClusterFunction Class: " + ex;
      throw new IllegalStateException(msg, ex);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  public static void printSystemPropsEnvVariables() {
    Map<String, String> env = System.getenv();
    System.out.println("Below are the environment variables: ");
    for (Map.Entry<String, String> mapEntry : env.entrySet()) {
       System.out.format("%s=%s%n", mapEntry.getKey(), mapEntry.getValue());
    }
    Properties p = System.getProperties();
    System.out.println("\n\n Below are the Java system properties: ");
    for (Map.Entry<Object, Object> mapEntry : p.entrySet()) {
      System.out.format("%s=%s%n", (String)mapEntry.getKey(), (String)mapEntry.getValue());
    }
  }
}
