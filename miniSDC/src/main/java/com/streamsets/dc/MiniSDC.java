/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc;


import com.streamsets.pipeline.BootstrapMain;
import com.streamsets.pipeline.DataCollector;
import com.streamsets.pipeline.SDCClassLoader;

import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class MiniSDC {
  // Duplicate this here from container
  public enum ExecutionMode {CLUSTER, STANDALONE, SLAVE};

  private String libraryRoot;
  private List<URL> apiUrls;
  private List<URL> containerUrls;
  private Map<String, List<URL>> streamsetsLibsUrls;
  private Map<String, List<URL>> userLibsUrls;
  private DataCollector dataCollector;

  public MiniSDC(String libraryRoot) {
    this.libraryRoot = libraryRoot;
  }

  public void startSDC() throws Exception {
    if (dataCollector != null) {
      throw new IllegalStateException("Data collector has already started");
    }
    apiUrls = BootstrapMain.getClasspathUrls(libraryRoot + "/api-lib/*.jar");
    containerUrls = BootstrapMain.getClasspathUrls(libraryRoot + "/container-lib/*.jar");
    streamsetsLibsUrls = BootstrapMain.getStageLibrariesClasspaths(libraryRoot + "/streamsets-libs");
    userLibsUrls = BootstrapMain.getStageLibrariesClasspaths(libraryRoot + "/user-libs");

    ClassLoader apiCL = SDCClassLoader.getAPIClassLoader(apiUrls, ClassLoader.getSystemClassLoader());
    ClassLoader containerCL = SDCClassLoader.getContainerCLassLoader(containerUrls, apiCL);
    List<ClassLoader> stageLibrariesCLs = new ArrayList<>();
    Map<String, List<URL>> libsUrls = new LinkedHashMap<>();
    libsUrls.putAll(streamsetsLibsUrls);
    libsUrls.putAll(userLibsUrls);
    for (Map.Entry<String, List<URL>> entry : libsUrls.entrySet()) {
      String[] parts = entry.getKey().split(BootstrapMain.FILE_SEPARATOR);
      if (parts.length != 2) {
        String msg = "Invalid library name: " + entry.getKey();
        throw new IllegalStateException(msg);
      }
      String type = parts[0];
      String name = parts[1];
      stageLibrariesCLs.add(SDCClassLoader.getStageClassLoader(type, name, entry.getValue(), apiCL));
    }

   injectStageLibraries(containerCL, stageLibrariesCLs);

    // Bootstrap container
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(containerCL);
      dataCollector =
        (DataCollector) Class.forName("com.streamsets.dc.datacollector.MiniITDataCollector", true, containerCL)
          .getConstructor().newInstance();
      dataCollector.init();
    } catch (Exception e) {
      e.printStackTrace();
    }  finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  public void createAndStartPipeline(String pipelineJson) throws Exception {
    if(dataCollector == null) {
      throw new IllegalStateException("DataCollector is not initialized.");
    }
    dataCollector.startPipeline(pipelineJson);
  }

  public void createPipeline(String pipelineJson) throws Exception {
    if(dataCollector == null) {
      throw new IllegalStateException("DataCollector is not initialized.");
    }
    dataCollector.createPipeline(pipelineJson);
  }

  public void startPipeline() throws Exception {
    if(dataCollector == null) {
      throw new IllegalStateException("DataCollector is not initialized.");
    }
    dataCollector.startPipeline();
  }

  public void stopPipeline() throws Exception {
    if(dataCollector == null) {
      throw new IllegalStateException("DataCollector is not initialized.");
    }
    dataCollector.stopPipeline();
  }

  private void injectStageLibraries(ClassLoader containerCL, List<ClassLoader> stageLibrariesCLs) {
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(containerCL);
      Class<?> runtimeModuleClz = Class.forName("com.streamsets.pipeline.main.RuntimeModule", true, containerCL);
      Method setStageLibraryClassLoadersMethod = runtimeModuleClz.getMethod("setStageLibraryClassLoaders", List.class);
      setStageLibraryClassLoadersMethod.invoke(null, stageLibrariesCLs);
    } catch (Exception ex) {
      String msg = "Error trying to bookstrap Spark while setting stage classloaders: " + ex;
      throw new IllegalStateException(msg, ex);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  public URI getServerURI() {
    return dataCollector.getServerURI();
  }

  public void stop() {
    dataCollector.destroy();
  }

  public List<URI> getListOfSlaveSDCURI() throws URISyntaxException {
    return dataCollector.getWorkerList();
  }

}
