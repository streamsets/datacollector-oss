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
package com.streamsets.datacollector;

import com.streamsets.pipeline.BootstrapMain;
import com.streamsets.pipeline.SDCClassLoader;
import com.streamsets.pipeline.impl.DataCollector;
import com.streamsets.pipeline.validation.ValidationIssue;

import java.io.IOException;
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

  private final String libraryRoot;
  private List<URL> apiUrls;
  private List<URL> containerUrls;
  private Map<String, List<URL>> streamsetsLibsUrls;
  private Map<String, List<URL>> userLibsUrls;
  private DataCollector dataCollector;
  private ClassLoader containerCL;

  public MiniSDC(String libraryRoot) {
    this.libraryRoot = libraryRoot;
  }

  public void startSDC() throws Exception {
    if (dataCollector != null) {
      throw new IllegalStateException("Data collector has already started");
    }
    apiUrls = BootstrapMain.getClasspathUrls(libraryRoot + "/api-lib/*.jar");
    containerUrls = BootstrapMain.getClasspathUrls(libraryRoot + "/container-lib/*.jar");
    streamsetsLibsUrls = BootstrapMain.getStageLibrariesClasspaths(libraryRoot + "/streamsets-libs", null, null, null);
    userLibsUrls = BootstrapMain.getStageLibrariesClasspaths(libraryRoot + "/user-libs", null, null, null);

    ClassLoader apiCL = SDCClassLoader.getAPIClassLoader(apiUrls, ClassLoader.getSystemClassLoader());
    containerCL = SDCClassLoader.getContainerCLassLoader(containerUrls, apiCL);
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
        (DataCollector) Class.forName("com.streamsets.datacollector.MiniITDataCollector", true, containerCL)
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

  public String createRules(String name, String tag, String rulesJson) throws Exception {
    if(dataCollector == null) {
      throw new IllegalStateException("DataCollector is not initialized.");
    }
    return dataCollector.storeRules(name, tag, rulesJson);
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
      Class<?> runtimeModuleClz = Class.forName("com.streamsets.datacollector.main.RuntimeModule", true, containerCL);
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
    if (dataCollector != null) {
      dataCollector.destroy();
    }
  }

  public List<URI> getListOfSlaveSDCURI() throws URISyntaxException {
    return dataCollector.getWorkerList();
  }

  public List<? extends ValidationIssue> validatePipeline(String name, String pipelineJson) throws IOException {
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(containerCL);
      return dataCollector.validatePipeline(name, pipelineJson);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }
}
