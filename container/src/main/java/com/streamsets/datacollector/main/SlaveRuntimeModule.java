/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.datacollector.main;


import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.runner.common.Constants;
import com.streamsets.datacollector.metrics.MetricsModule;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
import dagger.Module;
import dagger.Provides;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

@Module(library = true, injects = {BuildInfo.class, RuntimeInfo.class, Configuration.class, EventListenerManager
    .class}, includes = MetricsModule.class)
public class SlaveRuntimeModule {
  private static final Logger LOG = LoggerFactory.getLogger(SlaveRuntimeModule.class);
  public static final String SDC_PROPERTY_PREFIX = "sdc";
  private static List<ClassLoader> stageLibraryClassLoaders = Collections.EMPTY_LIST;

  // Called by BootstrapCluster through reflection
  public static synchronized void setStageLibraryClassLoaders(List<? extends ClassLoader> classLoaders) {
    stageLibraryClassLoaders = ImmutableList.copyOf(classLoaders);
  }

  @Provides
  @Singleton
  public BuildInfo provideBuildInfo() {
    return new DataCollectorBuildInfo();
  }

  @Provides
  @Singleton
  public RuntimeInfo provideRuntimeInfo(MetricRegistry metrics) {
    RuntimeInfo info = new SlaveRuntimeInfo(SDC_PROPERTY_PREFIX, metrics, stageLibraryClassLoaders);
    info.init();
    return info;
  }

  @Provides
  @Singleton
  public Configuration provideConfiguration(RuntimeInfo runtimeInfo) {
    Configuration.setFileRefsBaseDir(new File(runtimeInfo.getConfigDir()));
    Configuration conf = new Configuration();
    File configFile = new File(runtimeInfo.getConfigDir(), "sdc.properties");
    if (configFile.exists()) {
      try (FileReader reader = new FileReader(configFile)) {
        conf.load(reader);
        ((SlaveRuntimeInfo) runtimeInfo).setMasterSDCId(Utils.checkNotNull(conf.get(Constants.SDC_ID, null),
            Constants.SDC_ID
        ));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    } else {
      LOG.error("Error did not find sdc.properties at expected location: {}", configFile);
    }
    return conf;
  }

  @Provides
  @Singleton
  public EventListenerManager provideEventListenerManager() {
    return new EventListenerManager();
  }
}
