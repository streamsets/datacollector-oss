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

import com.streamsets.datacollector.cluster.ClusterModeConstants;
import com.streamsets.pipeline.spark.api.SparkTransformer;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

/**
 * This class is responsible for all activities which cross classloaders. At present
 * there are two use cases for this class:
 * <ol>
 *   <li>Bootstrapping an Executor which is started as part of a spark job</li>
 *   <li>Obtaining a reference on the dummy source which is used to feed a pipeline</li>
 * </ol>
 */
public class BootstrapCluster {
  public static final String STREAMSETS_LIBS_PREFIX = "streamsets-libs/";
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
  private static final String MESOS_BOOTSTRAP_JAR_REGEX = "streamsets-datacollector-mesos-bootstrap";
  public static final String SDC_MESOS_BASE_DIR = "sdc_mesos";
  private static File mesosBootstrapFile;
  private static List<ClassLoader> transformerCLs;
  private static String pipelineJson;
  private static List<SparkTransformer> transformers;

  private BootstrapCluster() {}

  public static synchronized Properties getProperties() throws Exception {
    initialize();
    return properties;
  }

  private static synchronized void initialize() throws Exception {
    if (initialized) {
      return;
    }
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
    try (FileInputStream inStream = new FileInputStream(sdcProperties)) {
      properties.load(inStream);
    }
    File rootDataDir = new File(etcRoot, "data");
    dataDir = rootDataDir.getAbsolutePath();
    File basePipelineDir = new File(rootDataDir, "pipelines");
    String pipelineName = properties.getProperty(ClusterModeConstants.CLUSTER_PIPELINE_NAME);
    if (pipelineName == null) {
      throw new IllegalStateException("Pipeline to be run cannot be null");
    }
    SDCClassLoader.setDebug(Boolean.getBoolean(BootstrapMain.PIPELINE_BOOTSTRAP_CLASSLOADER_SYS_PROP));

    List<URL> apiUrls;
    List<URL> containerUrls;
    Map<String, List<URL>> streamsetsLibsUrls;
    Map<String, List<URL>> userLibsUrls;
    if (isTestingMode) {
      apiUrls = new ArrayList<>();
      containerUrls = new ArrayList<>();
      streamsetsLibsUrls = new HashMap<>();
      userLibsUrls = new HashMap<>();
      // for now we pull in container in for testing mode
      streamsetsLibsUrls.put("streamsets-libs/streamsets-datacollector-spark-protolib",
        BootstrapMain.getClasspathUrls(System.getProperty("user.dir") + "/target/"));
    } else {
      apiUrls = BootstrapMain.getClasspathUrls(libraryRoot + "/api-lib/*.jar");
      containerUrls = BootstrapMain.getClasspathUrls(libraryRoot + "/container-lib/*.jar");

      Set<String> systemStageLibs;
      Set<String> userStageLibs;
      if (BootstrapMain.isDeprecatedWhiteListConfiguration(etcRoot)) {
        System.out.println(String.format(
            BootstrapMain.WARN_MSG,
            "Using deprecated stage library whitelist configuration file",
            BootstrapMain.WHITE_LIST_FILE
        ));
        systemStageLibs = BootstrapMain.getWhiteList(etcRoot, BootstrapMain.SYSTEM_LIBS_WHITE_LIST_KEY);
        userStageLibs = BootstrapMain.getWhiteList(etcRoot, BootstrapMain.USER_LIBS_WHITE_LIST_KEY);
      } else {
        systemStageLibs = BootstrapMain.getSystemStageLibs(etcRoot);
        userStageLibs = BootstrapMain.getUserStageLibs(etcRoot);
      }

      String libsCommonLibDir = libraryRoot + "/libs-common-lib";

      // in cluster mode, the library extra dir files from the master are collapsed on the library dir
      streamsetsLibsUrls = BootstrapMain.getStageLibrariesClasspaths(libraryRoot + "/streamsets-libs", null,
                                                                     systemStageLibs, libsCommonLibDir);
      userLibsUrls = BootstrapMain.getStageLibrariesClasspaths(libraryRoot + "/user-libs", null, userStageLibs,
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
    try {
      pipelineJson = new String(Files.readAllBytes(Paths.get(pipelineJsonFile.toURI())), StandardCharsets.UTF_8);
    } catch (Exception ex) {
      String msg = "Error reading Pipeline JSON File at: " + pipelineJsonFile;
      throw new IllegalStateException(msg, ex);
    }

    String sparkLib = getSourceLibraryName();
    List<String> sparkProcessorLibs =
        getSparkProcessorLibraryNames()
        .stream()
            .map(x -> STREAMSETS_LIBS_PREFIX + x).collect(Collectors.toList());
    if (sparkLib == null) {
      throw new IllegalStateException("Couldn't find the source library in pipeline file");
    }
    String lookupLib = STREAMSETS_LIBS_PREFIX + sparkLib;
    System.err.println("\n Cluster lib is " + lookupLib);
    for (Map.Entry<String,List<URL>> entry : libsUrls.entrySet()) {
      String[] parts = entry.getKey().split(System.getProperty("file.separator"));
      if (parts.length != 2) {
        String msg = "Invalid library name: " + entry.getKey();
        throw new IllegalStateException(msg);
      }
      String type = parts[0];
      String name = parts[1];
      SDCClassLoader sdcClassLoader = SDCClassLoader.getStageClassLoader(type, name, entry.getValue(), apiCL); //NOSONAR
      // TODO add spark, scala, etc to blacklist
      if (lookupLib.equals(entry.getKey())) {
        if (sparkCL != null) {
          throw new IllegalStateException("Found two classloaders for " + lookupLib);
        }
        sparkCL = sdcClassLoader;
      }

      if (sparkProcessorLibs.contains(entry.getKey())) {
        if (transformerCLs == null) {
          transformerCLs = new ArrayList<>();
        }
        transformerCLs.add(sdcClassLoader);
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
      Class<?> runtimeModuleClz = Class.forName("com.streamsets.datacollector.main.SlaveRuntimeModule", true,
          containerCL);
      Method setStageLibraryClassLoadersMethod = runtimeModuleClz.getMethod("setStageLibraryClassLoaders", List.class);
      setStageLibraryClassLoadersMethod.invoke(null, stageLibrariesCLs);
    } catch (Exception ex) {
      String msg = "Error trying to bookstrap Spark while setting stage classloaders: " + ex;
      throw new IllegalStateException(msg, ex);
    }
    initialized = true;
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
   * @param postBatchRunnable runnable to run after each batch is finished
   * @return a source object associated with the newly created pipeline
   * @throws Exception
   */
  public static /*PipelineStartResult*/ Object startPipeline(Runnable postBatchRunnable) throws Exception {
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

  private static String getSourceLibraryName() throws Exception {
    try {
      return callOnPiplineConfigurationUtil("getSourceLibName");
    } catch (Exception ex) {
      String msg = "Error trying to retrieve library name from pipeline json: " + ex;
      throw new IllegalStateException(msg, ex);
    }
  }

  public static List<String> getSparkProcessorLibraryNames() throws Exception {
    return callOnPiplineConfigurationUtil("getSparkProcessorConf");
  }

  @SuppressWarnings("unchecked")
  private static <T> T callOnPiplineConfigurationUtil(String method) throws Exception {
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(containerCL);
      Class pipelineConfigurationUtil =
          Class.forName("com.streamsets.datacollector.util.PipelineConfigurationUtil", true, containerCL);
      Method m = pipelineConfigurationUtil.getMethod(method, String.class);
      return (T) m.invoke(null, pipelineJson);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  @SuppressWarnings("unchecked")
  public static void createTransformers(JavaSparkContext context) throws Exception {
    transformers = new ArrayList<>();
    if (transformerCLs == null) {
      return;
    }

    List<Object> configs = callOnPiplineConfigurationUtil("getSparkTransformers");

    for (Object transformerConfig : configs) {
      try {
        String transformerClass =
            (String) transformerConfig.getClass().getMethod("getTransformerClass").invoke(transformerConfig);
        Class<?> clazz = Class.forName(transformerClass);
        SparkTransformer transformer = (SparkTransformer) clazz.newInstance();
        List<String> params =
            (List<String>) transformerConfig.getClass().
                getMethod("getTransformerParameters").invoke(transformerConfig);
        transformer.init(context, params);
        transformers.add(transformer);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public static List<SparkTransformer> getTransformers() {
    return transformers;
  }

  public static Object getClusterFunction(String id) throws Exception {
    BootstrapCluster.initialize();
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(sparkCL);
      return Class.forName("com.streamsets.pipeline.cluster.ClusterFunctionImpl", true, sparkCL)
          .getMethod("create", Properties.class, String.class, String.class)
          .invoke(null, properties, id, dataDir);
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

  private static class MesosBootstrapJarFileFilter implements FilenameFilter {
    @Override
    public boolean accept(File dir, String name) {
      return name.startsWith(MESOS_BOOTSTRAP_JAR_REGEX);
    }
  }

  public static File getMesosBootstrapFile() {
    if (mesosBootstrapFile == null) {
      throw new IllegalStateException("Mesos bootstrap file cannot be found");
    }
    return mesosBootstrapFile;
  }

  public static int findAndExtractJar(File mesosHomeDir, File sparkHomeDir) throws IOException, InterruptedException {
    FilenameFilter mesosBootstrapJarFilter = new MesosBootstrapJarFileFilter();
    File[] mesosBootstrapFile = mesosHomeDir.listFiles(mesosBootstrapJarFilter);
    checkNotNull(mesosBootstrapFile, mesosHomeDir);
    if (mesosBootstrapFile.length == 0) {
      mesosBootstrapFile = sparkHomeDir.listFiles(mesosBootstrapJarFilter);
    }
    checkNotNull(mesosBootstrapFile, sparkHomeDir);
    if (mesosBootstrapFile.length == 0) {
      throw new IllegalStateException("Cannot find file starting with " + MESOS_BOOTSTRAP_JAR_REGEX + " in "
        + sparkHomeDir + " or in " + mesosHomeDir);
    } else if (mesosBootstrapFile.length > 1) {
      throw new IllegalStateException("Found more than one file matching " + MESOS_BOOTSTRAP_JAR_REGEX
        + "; list of files are: " + Arrays.toString(mesosBootstrapFile));
    }
    File mesosBaseDir = new File(mesosHomeDir, SDC_MESOS_BASE_DIR);
    if (!mesosBaseDir.mkdir()) {
      throw new IllegalStateException("Error while creating dir: " + mesosBaseDir.getAbsolutePath());
    }
    BootstrapCluster.mesosBootstrapFile = mesosBootstrapFile[0];
    extractFromJar(mesosBootstrapFile[0], mesosBaseDir);
    return extractArchives(mesosBaseDir);
  }

  private static void checkNotNull(File[] mesosBootstrapFile, File sourceDir) {
    if (mesosBootstrapFile == null) {
      throw new IllegalStateException("Cannot list files in dir: " + sourceDir.getAbsolutePath());
    }
  }

  private static int extractArchives(File mesosBaseDir) throws IOException, InterruptedException {
   // Extract archives from the uber jar
    String[] cmd = {"/bin/bash", "-c",
          "cd " + mesosBaseDir.getAbsolutePath() + " && "
        + "tar -xf etc.tar.gz && "
        + "mkdir libs && "
        + "tar -xf libs.tar.gz -C libs/ && "
        + "tar -xf resources.tar.gz" };
    ProcessBuilder processBuilder = new ProcessBuilder(cmd);
    processBuilder.redirectErrorStream(true);
    Process process = processBuilder.start();
    try (BufferedReader stdOutReader =
      new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String line = null;
      while ((line = stdOutReader.readLine()) != null) {
        System.out.println(line);
      }
      process.waitFor();
    }
    return process.exitValue();
  }

  private static void extractFromJar(File sourceFile, File destDir) throws IOException {
    try(JarFile jar = new JarFile(sourceFile)) {
      Enumeration enumEntries = jar.entries();
      while (enumEntries.hasMoreElements()) {
        JarEntry jarEntry = (JarEntry) enumEntries.nextElement();
        java.io.File destFile = new java.io.File(destDir, jarEntry.getName());
        File parentDestFile = destFile.getParentFile();
        // if parent file does not exist, create the chain of dirs for this file
        if (!parentDestFile.isDirectory() && !parentDestFile.mkdirs()) {
          throw new IllegalStateException("Cannot create parent directories for file: " + destFile.getAbsolutePath());
        }
        if (jarEntry.isDirectory()) {
          continue;
        }
        try(
          InputStream is = jar.getInputStream(jarEntry);
          FileOutputStream fos = new java.io.FileOutputStream(destFile);
        ){
          byte[] buffer = new byte[8092];
          int bytesRead;
          while ((bytesRead = is.read(buffer)) != -1) { // write contents of 'is' to 'fos'
            fos.write(buffer, 0, bytesRead);
          }
        }
      }
    }
  }
}
