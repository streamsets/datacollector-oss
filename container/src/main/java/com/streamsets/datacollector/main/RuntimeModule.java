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
package com.streamsets.datacollector.main;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.http.WebServerTask;
import com.streamsets.datacollector.metrics.MetricsModule;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
import dagger.Module;
import dagger.Provides;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.File;
import java.util.Collections;
import java.util.List;

@Module(library = true, injects = {
    BuildInfo.class,
    RuntimeInfo.class,
    Configuration.class,
    EventListenerManager.class,
    UserGroupManager.class
}, includes = MetricsModule.class)
public class RuntimeModule {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeModule.class);
  public static final String SDC_PROPERTY_PREFIX = "sdc";
  public static final String PIPELINE_EXECUTION_MODE_KEY = "pipeline.execution.mode";
  private static List<ClassLoader> stageLibraryClassLoaders = Collections.emptyList();//ImmutableList.of(RuntimeModule.class.getClassLoader());

  public static synchronized void setStageLibraryClassLoaders(List<? extends ClassLoader> classLoaders) {
    stageLibraryClassLoaders = ImmutableList.copyOf(classLoaders);
  }

  @Provides @Singleton
  public BuildInfo provideBuildInfo() {
    return new DataCollectorBuildInfo();
  }

  @Provides @Singleton
  public RuntimeInfo provideRuntimeInfo(MetricRegistry metrics) {
    RuntimeInfo info = new StandaloneRuntimeInfo(SDC_PROPERTY_PREFIX, metrics, stageLibraryClassLoaders);
    info.init();
    return info;
  }

  @Provides @Singleton
  public Configuration provideConfiguration(RuntimeInfo runtimeInfo) {
    Configuration.setFileRefsBaseDir(new File(runtimeInfo.getConfigDir()));
    Configuration conf = new Configuration();
    RuntimeInfo.loadOrReloadConfigs(runtimeInfo, conf);
    return conf;
  }

  @Provides @Singleton
  public EventListenerManager provideEventListenerManager() {
    return new EventListenerManager();
  }

  @Provides @Singleton
  public UserGroupManager provideUserGroupManager(Configuration configuration) {
    String loginModule = configuration.get(
        WebServerTask.HTTP_AUTHENTICATION_LOGIN_MODULE,
        WebServerTask.HTTP_AUTHENTICATION_LOGIN_MODULE_DEFAULT
    );
    switch (loginModule) {
      case WebServerTask.FILE:
        return new FileUserGroupManager();
      case WebServerTask.LDAP:
        return new LdapUserGroupManager();
      default:
        throw new RuntimeException(Utils.format("Invalid Authentication Login Module '{}', must be one of '{}'",
            loginModule, WebServerTask.LOGIN_MODULES));
    }
  }

}
