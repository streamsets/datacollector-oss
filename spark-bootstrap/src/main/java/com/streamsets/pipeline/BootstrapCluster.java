/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
  private static String pipelineJson;
  private static ClassLoader apiCL;
  private static ClassLoader containerCL;
  private static ClassLoader sparkCL;
  private static List<ClassLoader> stageLibrariesCLs;

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
    } else {
      libraryRoot = (new File(System.getProperty("user.dir"), "libs.tar.gz")).getAbsolutePath();
      etcRoot = (new File(System.getProperty("user.dir") + "/etc.tar.gz/etc/")).getAbsolutePath();
      resourcesRoot = (new File(System.getProperty("user.dir") + "/resources.tar.gz/resources/")).getAbsolutePath();
    }
    System.setProperty("sdc.clustermode", "true");
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
    SDCClassLoader.setDebug(Boolean.getBoolean("pipeline.bootstrap.debug") ||
      Boolean.parseBoolean(properties.getProperty("pipeline.bootstrap.debug")));
    File pipelineJsonFile = new File(etcRoot, "pipeline.json");
    if (!pipelineJsonFile.isFile()) {
      String msg = "Pipeline JSON file does not exist at expected location: " + pipelineJsonFile;
      throw new IllegalStateException(msg);
    }
    try {
      pipelineJson = new String(Files.readAllBytes(Paths.get(pipelineJsonFile.toURI())), StandardCharsets.UTF_8);
    } catch (Exception ex) {
      String msg = "Error reading Pipeline JSON File at: " + pipelineJsonFile;
      throw new IllegalStateException(msg, ex);
    }
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
      streamsetsLibsUrls = BootstrapMain.getStageLibrariesClasspaths(libraryRoot +
        "/streamsets-libs");
      userLibsUrls = BootstrapMain.getStageLibrariesClasspaths(libraryRoot + "/user-libs");
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

    stageLibrariesCLs = new ArrayList<ClassLoader>();
    String sparkLib = getSourceLibraryName(pipelineJson);
    if (sparkLib == null) {
      throw new IllegalStateException("Couldn't find the source library in pipeline file");
    }
    String lookupLib = "streamsets-libs" +"/" + sparkLib;
    System.out.println("\n Lookup lib is " + lookupLib);
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
        Method memoryUsageCollectorInitialize = Class.forName("com.streamsets.pipeline.memory.MemoryUsageCollector",
          true, containerCL).getMethod("initialize", Instrumentation.class);
        memoryUsageCollectorInitialize.invoke(null, instrumentation);
      }
    } catch (Exception ex) {
      String msg = "Error trying to initialize MemoryUsageCollector: " + ex;
      throw new IllegalStateException(msg, ex);
    }
    try {
      Class<?> runtimeModuleClz = Class.forName("com.streamsets.pipeline.main.RuntimeModule", true, containerCL);
      Method setStageLibraryClassLoadersMethod = runtimeModuleClz.getMethod("setStageLibraryClassLoaders", List.class);
      setStageLibraryClassLoadersMethod.invoke(null, stageLibrariesCLs);
    } catch (Exception ex) {
      String msg = "Error trying to bookstrap Spark while setting stage classloaders: " + ex;
      throw new IllegalStateException(msg, ex);
    }
  }

  /**
   * Bootstrapping the Driver which starts a spark job on cluster
   */
  public static void main(String[] args) throws Exception {
    BootstrapCluster.initialize();
    DelegatingClusterBinding binding = new DelegatingClusterBinding(properties, pipelineJson);
    try {
      binding.init();
      binding.awaitTermination();
    } catch (Exception ex) {
      String msg = "Error trying to invoke SparkStreamingBinding.main: " + ex;
      throw new IllegalStateException(msg, ex);
    } finally {
      binding.close();
    }
  }

  /**
   * Obtaining a reference on the dummy source which is used to feed a pipeline<br/>
   * Direction: Stage -> Container
   * @param postBatchRunnable
   * @return a source object associated with the newly created pipeline
   * @throws Exception
   */
  public static /*Source*/ Object createPipeline(Properties properties, String pipelineJson,
                                                 Runnable postBatchRunnable) throws Exception {
    BootstrapCluster.initialize();
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(containerCL);
      Class embeddedPipelineFactoryClz = Class.forName("com.streamsets.pipeline.datacollector.EmbeddedDataCollectorFactory", true,
        containerCL);
      Method createPipelineMethod = embeddedPipelineFactoryClz.getMethod("createPipeline", Properties.class,
        String.class, Runnable.class);
      return createPipelineMethod.invoke(null, properties, pipelineJson, postBatchRunnable);
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
        Class.forName("com.streamsets.pipeline.util.PipelineConfigurationUtil", true, containerCL);
      Method createPipelineMethod = pipelineConfigurationUtil.getMethod("getSourceLibName", String.class);
      return (String) createPipelineMethod.invoke(null, pipelineJson);
    } catch (Exception ex) {
      String msg = "Error trying to retrieve library name from pipeline json: " + ex;
      throw new IllegalStateException(msg, ex);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }


  /**
   * Bootstrapping an Executor which is started as part of a spark kafka/hdfs job<br/>
   * Direction: Spark Executor -> Stage
   * @return an instance of the real SparkExecutorFunction
   * @throws Exception
   */
  public static Method getSparkExecutorFunction() throws Exception {
    BootstrapCluster.initialize();
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(sparkCL);
      return Class.forName("com.streamsets.pipeline.spark.SparkExecutorFunction", true,
        sparkCL).getMethod("execute", Properties.class, String.class, Iterator.class);
    } catch (Exception ex) {
      String msg = "Error trying to obtain SparkExecutorFunction Class: " + ex;
      throw new IllegalStateException(msg, ex);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }
}
